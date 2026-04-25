package algorithm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"recon/api/boostevent"
	"recon/api/kubeapi"
	"recon/api/kubeconfig"
	"recon/api/logging"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	DYNCLIENT = kubeconfig.DYNCLIENT
	CLIENTSET = kubeconfig.CLIENTSET
	STD_GVR   = kubeconfig.STD_GVR
	RECON_GVR = kubeconfig.RECON_GVR
)

const (
	defaultColdSamples = 1
	defaultWarmSamples = 10
	probeRetryInterval = 2 * time.Second
	// interSampleSleep is the cool-down between samples within one probe.
	// Needed because force-deleting the pod only clears it from the k8s API
	// — the node-level container and Knative's endpoint routing take a few
	// seconds to catch up. Without this delay, the next curl can land on
	// the still-terminating warm pod and record a bogus sub-second "cold
	// start". 10 s empirically covers both the kubelet grace and Knative's
	// endpoint-propagation delay. Also gives queue-proxy time to recover
	// from transient errors between samples.
	interSampleSleep = 10 * time.Second

	// probeTimeout caps a single triggerHttp attempt. If the pod's
	// queue-proxy or app deadlocks, the curl loop would otherwise retry
	// forever. Set generously enough for legitimate cold starts at low
	// CPU (yolo at 200m takes ~45 s) but short enough to catch genuine
	// stuck pods.
	probeTimeout = 120 * time.Second

	// maxStuckRetries is the number of attempts per cold sample before we
	// give up. Each attempt deletes the existing pod, waits for scale to
	// zero, and re-triggers — so a stuck pod is replaced rather than
	// indefinitely retried.
	maxStuckRetries = 3

	phaseCold = "COLD"
	phaseWarm = "WARM"
)

// sleepCtx waits for d or for ctx cancellation, whichever comes first.
// Returns nil on full duration, ctx.Err() on cancellation — so callers can
// propagate shutdown without a hanging time.Sleep.
func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func buildLabelSelector(ev *boostevent.BoostEvent) string {
	var parts []string
	for _, expr := range ev.Selector.MatchExpressions {
		vals := strings.Join(expr.Values, ",")
		parts = append(parts, fmt.Sprintf("%s %s (%s)", expr.Key, strings.ToLower(expr.Operator), vals))
	}
	return strings.Join(parts, ",")
}

// coldSampleWithStuckRecovery runs one cold-start sample with auto-recovery:
// delete leftover pods, wait for scale to zero, cool-down, then trigger the
// curl under a probeTimeout deadline. If the curl exhausts probeTimeout
// without a successful response (queue-proxy or app stuck), the function
// deletes the pod and retries up to maxStuckRetries times. After that it
// gives up so BinarySearch can abort cleanly instead of hanging forever.
func coldSampleWithStuckRecovery(
	ctx context.Context,
	event *boostevent.BoostEvent,
	labelSelector, targetKsvc string,
	sampleIdx, sampleTotal int,
) (time.Duration, error) {
	for attempt := 1; attempt <= maxStuckRetries; attempt++ {
		// Force-delete any leftover pod from a previous probe — the upstream
		// boost webhook only fires on creation, so a pod carrying an old CPU
		// limit cannot be re-mutated, it can only be replaced.
		if err := kubeapi.DeleteKsvcPods(ctx, event.Metadata.Namespace, labelSelector); err != nil {
			return 0, err
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
		// Anything else (network error not caused by deadline, malformed URL, etc.)
		return 0, err
	}
	return 0, fmt.Errorf("[COLD] sample %d/%d gave up after %d stuck retries", sampleIdx, sampleTotal, maxStuckRetries)
}

// waitForScaleToZero polls until no pods match labelSelector, or ctx is
// cancelled, or the API returns a non-recoverable error.
func waitForScaleToZero(ctx context.Context, phase, namespace, labelSelector string) error {
	logging.Warning(fmt.Sprintf("[%s] waiting for pods to scale to 0 (ns=%s)", phase, namespace))
	for {
		pods, err := CLIENTSET.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labelSelector,
		})
		if err != nil {
			return err
		}
		if len(pods.Items) == 0 {
			logging.Success(fmt.Sprintf("[%s] pods scaled to 0", phase))
			return nil
		}
		if err := sleepCtx(ctx, probeRetryInterval); err != nil {
			return err
		}
	}
}

func getResptCold(ctx context.Context, event *boostevent.BoostEvent, cpuValue string) (time.Duration, error) {
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

	// StartupCPUBoost stays alive for the whole probe so each sample's fresh
	// pod is injected with cpuValue by the upstream webhook. The resource
	// monitor, in contrast, is started per sample only during triggerHttp —
	// keeping it alive across waitForScaleToZero just spams logs with the
	// same leftover pod state every 2 s.
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

func getResptWarm(ctx context.Context, event *boostevent.BoostEvent, cpuValue string, cpuCold string) (time.Duration, error) {
	logging.Stage(fmt.Sprintf("[WARM] probe starting — cpu=%s cold_boost=%s ns=%s", cpuValue, cpuCold, event.Metadata.Namespace))
	labelSelector := buildLabelSelector(event)

	deployments, err := CLIENTSET.AppsV1().Deployments(event.Metadata.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		logging.Failure("[WARM] failed to list deployments:", err)
		return 0, err
	}
	if len(deployments.Items) == 0 {
		return 0, fmt.Errorf("no deployments match selector %q in namespace %q",
			labelSelector, event.Metadata.Namespace)
	}

	d := deployments.Items[0]
	for _, container := range d.Spec.Template.Spec.Containers {
		if container.Name == "user-container" {
			oldCPU := container.Resources.Limits.Cpu()
			logging.Info(fmt.Sprintf("[WARM] ksvc CPU limit change requested: container=%s old=%s new=%s",
				container.Name, oldCPU.String(), cpuValue))
		}
	}

	targetKsvc := event.Selector.MatchExpressions[0].Values[0]
	if err := kubeapi.PatchResourceLimits(ctx, event.Metadata.Namespace, targetKsvc, cpuValue); err != nil {
		logging.Failure("[WARM] failed to patch ksvc CPU limit:", err)
		return 0, err
	}
	// maxScale=1 is set once by BinarySearch before the starting phase and
	// unset via defer when the whole search returns — it spans both phases so
	// every probe measures a single pod. Do not touch it here.

	logging.Info(fmt.Sprintf("[WARM] new ksvc revision rolled for %s/%s", event.Metadata.Namespace, targetKsvc))
	if err := waitForScaleToZero(ctx, phaseWarm, event.Metadata.Namespace, labelSelector); err != nil {
		return 0, err
	}

	kubeapi.CreateStartupCPUBoost(ctx, event, cpuCold)
	defer kubeapi.DeleteStartupCPUBoost(ctx, event.Metadata.Namespace, event.Metadata.Name)

	monCtx, monCancel := context.WithCancel(ctx)
	defer monCancel()
	go kubeapi.MonitorKsvcResources(monCtx, phaseWarm, event.Metadata.Namespace, targetKsvc)

	// Warmup: bring the pod up once before starting timed samples.
	logging.Info("[WARM] warmup curl before timed samples")
	if _, err := triggerHttp(ctx, phaseWarm, event.Spec.DurationPolicy.ApiCondition); err != nil {
		return 0, err
	}

	n := event.Spec.Measurement.WarmSamples
	if n < 1 {
		n = defaultWarmSamples
	}
	logging.Info(fmt.Sprintf("[WARM] samples to collect: %d", n))

	var sum time.Duration
	for i := 0; i < n; i++ {
		rt, err := triggerHttp(ctx, phaseWarm, event.Spec.DurationPolicy.ApiCondition)
		if err != nil {
			return 0, err
		}
		logging.Normal(fmt.Sprintf("[WARM] sample %d/%d: cpu=%s rt=%s", i+1, n, cpuValue, rt))
		sum += rt
		if i < n-1 {
			logging.Normal(fmt.Sprintf("[WARM] cool-down %s before next sample", interSampleSleep))
			if err := sleepCtx(ctx, interSampleSleep); err != nil {
				return 0, err
			}
		}
	}

	avg := sum / time.Duration(n)
	logging.Normal(fmt.Sprintf("[WARM] probe complete — cpu=%s avg=%s over %d samples", cpuValue, avg, n))
	return avg, nil
}

// triggerHttp repeatedly GETs api_condition.Url until the response body
// contains the expected substring. The phase tag ("COLD" / "WARM") shows
// up in every log line so you can tell at a glance which measurement is
// in flight. Honors ctx: cancellation aborts both in-flight requests and
// the inter-retry wait so SIGINT stops the probe immediately.
func triggerHttp(ctx context.Context, phase string, api_condition boostevent.ApiCondition) (time.Duration, error) {
	targetURL := api_condition.Url
	expectedResponse := api_condition.Response
	logging.Normal(fmt.Sprintf("[%s] curl GET %s (expect body~%q)", phase, targetURL, expectedResponse))

	client := &http.Client{Timeout: 5 * time.Second}
	start := time.Now()

	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return 0, err
		}
		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			logging.Normal(fmt.Sprintf("[%s] pod not reachable yet, retrying...", phase))
			if err := sleepCtx(ctx, probeRetryInterval); err != nil {
				return 0, err
			}
			continue
		}

		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			logging.Warning(fmt.Sprintf("[%s] failed to read response body, retrying...", phase))
			if err := sleepCtx(ctx, probeRetryInterval); err != nil {
				return 0, err
			}
			continue
		}

		if strings.Contains(string(bodyBytes), expectedResponse) {
			duration := time.Since(start)
			logging.Success(fmt.Sprintf("[%s] expected response received in %s", phase, duration))
			return duration, nil
		}
	}
}
