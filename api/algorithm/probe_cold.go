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

// getResptCold runs measurement.coldSamples cold-start probes at cpuValue
// and returns the mean response time. The StartupCPUBoost CR is created
// once (so each fresh pod is injected with cpuValue by the upstream
// webhook) and torn down on return; the resource monitor is started per
// sample only during triggerHttp.
func getResptCold(ctx context.Context, event *nimbusevent.NimbusEvent, cpuValue string) (time.Duration, error) {
	logging.Stage(fmt.Sprintf("[COLD] probe starting — cpu=%s ns=%s", cpuValue, event.Metadata.Namespace))
	labelSelector := buildLabelSelector(event)

	deployments, err := CLIENTSET.AppsV1().Deployments(event.Metadata.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		logging.Failure("[COLD] failed to list deployments:", err)
		return 0, err
	}
	if len(deployments.Items) == 0 {
		return 0, fmt.Errorf("no deployments match selector %q in namespace %q",
			labelSelector, event.Metadata.Namespace)
	}

	kubeapi.CreateStartupCPUBoost(ctx, event, cpuValue)
	defer kubeapi.DeleteStartupCPUBoost(ctx, event.Metadata.Namespace, event.Metadata.Name)

	n := event.Spec.Measurement.ColdSamples
	if n < 1 {
		n = defaultColdSamples
	}
	logging.Info(fmt.Sprintf("[COLD] samples to collect: %d", n))

	targetKsvc := event.Selector.MatchExpressions[0].Values[0]

	var sum time.Duration
	for i := 0; i < n; i++ {
		rt, err := coldSampleWithStuckRecovery(ctx, event, labelSelector, targetKsvc, i+1, n)
		if err != nil {
			return 0, err
		}
		logging.Normal(fmt.Sprintf("[COLD] sample %d/%d: cpu=%s rt=%s", i+1, n, cpuValue, rt))
		sum += rt
	}

	avg := sum / time.Duration(n)
	logging.Normal(fmt.Sprintf("[COLD] probe complete — cpu=%s avg=%s over %d samples", cpuValue, avg, n))
	return avg, nil
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
	labelSelector, targetKsvc string,
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
		rt, err := triggerHttp(probeCtx, phaseCold, event.Spec.DurationPolicy.ApiCondition)
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
