package algorithm

import (
	"context"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"nimbus/api/kubeapi"
	"nimbus/api/logging"
	"nimbus/api/nimbusevent"
)

const (
	// probeTimeout caps a single triggerHttp attempt. If the pod's
	// queue-proxy or app deadlocks, the curl loop would otherwise retry
	// forever. Set generously enough for legitimate cold starts at low
	// CPU but short enough to catch genuinely stuck pods.
	probeTimeout = 120 * time.Second

	// maxStuckRetries is the per-sample attempt budget. Each attempt
	// deletes the existing pod, waits for scale-to-zero, and re-triggers,
	// so a stuck pod is replaced rather than indefinitely retried.
	maxStuckRetries = 3
)

// SampleSink, when non-nil, is invoked once per individual sample with
// the measured response time. Used by the export pipeline to stream raw
// per-sample CSV rows without ever holding the samples in a slice. nil
// disables the side-channel; getResptCold/getResptWarm work the same.
type SampleSink func(rt time.Duration)

// getResptCold runs measurement.coldSamples cold-start probes at cpuValue
// and returns ProbeStats {Avg, P90, P95}. When onSample is non-nil, it is
// invoked once per individual sample as soon as the measurement is taken
// (used by the export pipeline; see internal/export). The StartupCPUBoost
// CR is created once (so each fresh pod is injected with cpuValue by the
// upstream webhook) and torn down on return; the resource monitor is
// started per sample only during triggerHttp.
func getResptCold(ctx context.Context, event *nimbusevent.NimbusEvent, cpuValue string, onSample SampleSink) (ProbeStats, error) {
	logging.Stage(fmt.Sprintf("[COLD] probe starting — cpu=%s ns=%s", cpuValue, event.Metadata.Namespace))
	labelSelector := buildLabelSelector(event)

	deployments, err := CLIENTSET.AppsV1().Deployments(event.Metadata.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		logging.Failure("[COLD] failed to list deployments:", err)
		return ProbeStats{}, err
	}
	if len(deployments.Items) == 0 {
		return ProbeStats{}, fmt.Errorf("no deployments match selector %q in namespace %q",
			labelSelector, event.Metadata.Namespace)
	}

	targetKsvc := event.Selector.MatchExpressions[0].Values[0]
	boostName := event.Metadata.Name + "-" + targetKsvc

	kubeapi.CreateStartupCPUBoost(ctx, event, targetKsvc, cpuValue)
	defer kubeapi.DeleteStartupCPUBoost(ctx, event.Metadata.Namespace, boostName)

	// targetURL is built from Values[0] (the one ksvc the cold phase
	// measures) + the user-supplied coldApiCondition.path; the upstream
	// boost webhook polls the same URL via its own probe.
	targetURL := kubeapi.BuildKsvcStatusURL(event.Metadata.Namespace, targetKsvc, event.Spec.DurationPolicy.ColdApiCondition.Path)

	n := event.Spec.Measurement.ColdSamples
	if n < 1 {
		n = defaultColdSamples
	}
	logging.Info(fmt.Sprintf("[COLD] samples to collect: %d", n))

	// Buffer the N raw samples locally so we can compute percentiles at
	// end-of-loop. Released when this function returns (peak ~N*8 bytes).
	samples := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		rt, err := coldSampleWithStuckRecovery(ctx, event, labelSelector, targetKsvc, targetURL, i+1, n)
		if err != nil {
			return ProbeStats{}, err
		}
		logging.Normal(fmt.Sprintf("[COLD] sample %d/%d: cpu=%s rt=%s", i+1, n, cpuValue, rt))
		if onSample != nil {
			onSample(rt)
		}
		samples = append(samples, rt)
	}

	stats := computeProbeStats(samples)
	logging.Normal(fmt.Sprintf("[COLD] probe complete — cpu=%s avg=%s p90=%s p95=%s over %d samples",
		cpuValue, stats.Avg, stats.P90, stats.P95, n))
	return stats, nil
}

// coldSampleWithStuckRecovery runs one cold-start sample with auto-recovery:
// delete leftover pods, wait for scale-to-zero, cool-down, then trigger the
// curl under a probeTimeout deadline. If the curl exhausts probeTimeout
// without a healthy response (queue-proxy or app stuck), the function
// deletes the pod and retries up to maxStuckRetries times. After that it
// gives up so BinarySearch can abort cleanly instead of hanging forever.
func coldSampleWithStuckRecovery(
	ctx context.Context,
	event *nimbusevent.NimbusEvent,
	labelSelector, targetKsvc, targetURL string,
	sampleIdx, sampleTotal int,
) (time.Duration, error) {
	for attempt := 1; attempt <= maxStuckRetries; attempt++ {
		// Force-delete any leftover pod from a previous probe — the upstream
		// boost webhook only fires on creation, so a pod carrying an old CPU
		// limit cannot be re-mutated, only replaced.
		deleted, err := kubeapi.DeleteKsvcPods(ctx, event.Metadata.Namespace, labelSelector)
		if err != nil {
			return 0, err
		}
		for _, name := range deleted {
			logging.Info(fmt.Sprintf("[COLD] reset pod %s/%s before sample %d/%d",
				event.Metadata.Namespace, name, sampleIdx, sampleTotal))
		}
		if err := waitForScaleToZero(ctx, phaseCold, event.Metadata.Namespace, labelSelector); err != nil {
			return 0, err
		}

		// Cool-down AFTER scale-to-zero is confirmed: k8s reports 0 pods
		// well before the kubelet finishes container teardown and before
		// Knative removes the pod IP from the service endpoints.
		logging.Normal(fmt.Sprintf("[COLD] cool-down %s for endpoint propagation", interSampleSleep))
		if err := sleepCtx(ctx, interSampleSleep); err != nil {
			return 0, err
		}

		// Monitor only around the trigger window — stopped as soon as the
		// curl returns so the next iteration's wait is silent.
		monCtx, monCancel := context.WithCancel(ctx)
		go kubeapi.MonitorKsvcResources(monCtx, phaseCold, event.Metadata.Namespace, targetKsvc)

		probeCtx, probeCancel := context.WithTimeout(ctx, probeTimeout)
		rt, err := triggerHttp(probeCtx, phaseCold, targetURL, event.Spec.DurationPolicy.ColdApiCondition.Response)
		probeCancel()
		monCancel()

		if err == nil {
			return rt, nil
		}
		// Parent ctx cancelled (SIGINT, etc.) — bubble up immediately.
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		// probeTimeout elapsed without a healthy response → pod is stuck.
		// Loop top will delete it and try again.
		if errors.Is(err, context.DeadlineExceeded) {
			logging.Warning(fmt.Sprintf("[COLD] sample %d/%d attempt %d/%d: probe stuck after %s, deleting pod and retrying",
				sampleIdx, sampleTotal, attempt, maxStuckRetries, probeTimeout))
			continue
		}
		// Anything else (network error not caused by deadline, malformed URL).
		return 0, err
	}
	return 0, fmt.Errorf("[COLD] sample %d/%d gave up after %d stuck retries", sampleIdx, sampleTotal, maxStuckRetries)
}
