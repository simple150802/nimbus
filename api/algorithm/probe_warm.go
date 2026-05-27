package algorithm

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"nimbus/api/kubeapi"
	"nimbus/api/logging"
	"nimbus/api/nimbusevent"
)

// getResptWarm patches the ksvc CPU to cpuValue, waits for scale-to-zero,
// fires one warmup curl to bring a fresh pod up, then runs
// measurement.warmSamples timed curls and returns ProbeStats {Avg, P90,
// P95}. cpuCold is the boost CPU used during the cold-start of the warmup
// pod (the upstream webhook applies it via the StartupCPUBoost CR). When
// onSample is non-nil, it is invoked once per individual timed sample
// (NOT the warmup curl) so the export pipeline can stream raw rows; nil
// disables the side-channel.
//
// maxScale=1 is set once by BinarySearch and not touched here.
//
// No stuck-pod auto-recovery: a warm probe failure aborts the whole search
// so RunWorker can retry on the next tick.
func getResptWarm(ctx context.Context, event *nimbusevent.NimbusEvent, cpuValue string, cpuCold string, onSample SampleSink) (ProbeStats, error) {
	logging.Stage(fmt.Sprintf("[WARM] probe starting — cpu=%s cold_boost=%s ns=%s", cpuValue, cpuCold, event.Metadata.Namespace))
	labelSelector := buildLabelSelector(event)

	deployments, err := CLIENTSET.AppsV1().Deployments(event.Metadata.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		logging.Failure("[WARM] failed to list deployments:", err)
		return ProbeStats{}, err
	}
	if len(deployments.Items) == 0 {
		return ProbeStats{}, fmt.Errorf("no deployments match selector %q in namespace %q",
			labelSelector, event.Metadata.Namespace)
	}

	for _, container := range deployments.Items[0].Spec.Template.Spec.Containers {
		if container.Name == "user-container" {
			oldCPU := container.Resources.Limits.Cpu()
			logging.Info(fmt.Sprintf("[WARM] ksvc CPU limit change requested: container=%s old=%s new=%s",
				container.Name, oldCPU.String(), cpuValue))
		}
	}

	targetKsvc := event.Selector.MatchExpressions[0].Values[0]
	boostName := event.Metadata.Name + "-" + targetKsvc
	if _, err := kubeapi.PatchResourceLimits(ctx, event.Metadata.Namespace, targetKsvc, cpuValue); err != nil {
		logging.Failure("[WARM] failed to patch ksvc CPU limit:", err)
		return ProbeStats{}, err
	}

	logging.Info(fmt.Sprintf("[WARM] new ksvc revision rolled for %s/%s", event.Metadata.Namespace, targetKsvc))
	if err := waitForScaleToZero(ctx, phaseWarm, event.Metadata.Namespace, labelSelector); err != nil {
		return ProbeStats{}, err
	}

	kubeapi.CreateStartupCPUBoost(ctx, event, targetKsvc, cpuCold)
	defer kubeapi.DeleteStartupCPUBoost(ctx, event.Metadata.Namespace, boostName)

	monCtx, monCancel := context.WithCancel(ctx)
	defer monCancel()
	go kubeapi.MonitorKsvcResources(monCtx, phaseWarm, event.Metadata.Namespace, targetKsvc)

	// targetURL built per ksvc — Values[0] is the one the warm phase
	// measures. Uses WarmApiCondition (workload endpoint, status-code
	// gate + optional body substring) so warm RT actually scales with
	// CPU, instead of measuring /status flag-read latency.
	warm := event.Spec.DurationPolicy.WarmApiCondition
	targetURL := kubeapi.BuildKsvcStatusURL(event.Metadata.Namespace, targetKsvc, warm.Path)

	logging.Info("[WARM] warmup curl before timed samples")
	if _, err := triggerHttpWithCodeBody(ctx, phaseWarm, targetURL, warm.StatusCode, warm.BodyContains); err != nil {
		return ProbeStats{}, err
	}

	n := event.Spec.Measurement.WarmSamples
	if n < 1 {
		n = defaultWarmSamples
	}
	logging.Info(fmt.Sprintf("[WARM] samples to collect: %d", n))

	// Buffer the N timed samples locally so we can compute percentiles at
	// end-of-loop. Released when this function returns (peak ~N*8 bytes).
	samples := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		rt, err := triggerHttpWithCodeBody(ctx, phaseWarm, targetURL, warm.StatusCode, warm.BodyContains)
		if err != nil {
			return ProbeStats{}, err
		}
		logging.Normal(fmt.Sprintf("[WARM] sample %d/%d: cpu=%s rt=%s", i+1, n, cpuValue, rt))
		if onSample != nil {
			onSample(rt)
		}
		samples = append(samples, rt)
		if i < n-1 {
			logging.Normal(fmt.Sprintf("[WARM] cool-down %s before next sample", interSampleSleep))
			if err := sleepCtx(ctx, interSampleSleep); err != nil {
				return ProbeStats{}, err
			}
		}
	}

	stats := computeProbeStats(samples)
	logging.Normal(fmt.Sprintf("[WARM] probe complete — cpu=%s avg=%s p90=%s p95=%s over %d samples",
		cpuValue, stats.Avg, stats.P90, stats.P95, n))
	return stats, nil
}
