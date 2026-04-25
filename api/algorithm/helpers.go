package algorithm

import (
	"context"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"nimbus/api/kubeconfig"
	"nimbus/api/logging"
	"nimbus/api/nimbusevent"
)

// Re-export the kubeconfig globals so the rest of the package can use the
// short names without prefixing every call.
var (
	DYNCLIENT  = kubeconfig.DYNCLIENT
	CLIENTSET  = kubeconfig.CLIENTSET
	STD_GVR    = kubeconfig.STD_GVR
	NIMBUS_GVR = kubeconfig.NIMBUS_GVR
)

const (
	defaultColdSamples = 1
	defaultWarmSamples = 10

	// probeRetryInterval is the wait between (a) successive checks during
	// waitForScaleToZero and (b) successive curl retries inside triggerHttp.
	probeRetryInterval = 2 * time.Second

	// interSampleSleep is the cool-down between samples within one probe.
	// Force-deleting the pod only clears it from the k8s API — the
	// node-level container and Knative's endpoint routing take a few seconds
	// to catch up. Without this delay, the next curl can land on the still-
	// terminating warm pod and record a bogus sub-second "cold start".
	// 10 s empirically covers both kubelet grace and Knative endpoint
	// propagation. Also gives queue-proxy time to recover from transient
	// errors between samples.
	interSampleSleep = 10 * time.Second

	phaseCold = "COLD"
	phaseWarm = "WARM"
)

// sleepCtx waits for d or ctx cancellation, whichever comes first. Returns
// nil on full duration, ctx.Err() on cancellation — so callers propagate
// shutdown without a hanging time.Sleep.
func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// buildLabelSelector reconstructs the comma-joined label selector string
// from the Nimbus's matchExpressions. Used to query pods/deployments the
// upstream boost webhook will inject into.
func buildLabelSelector(ev *nimbusevent.NimbusEvent) string {
	var parts []string
	for _, expr := range ev.Selector.MatchExpressions {
		vals := strings.Join(expr.Values, ",")
		parts = append(parts, fmt.Sprintf("%s %s (%s)", expr.Key, strings.ToLower(expr.Operator), vals))
	}
	return strings.Join(parts, ",")
}

// waitForScaleToZero polls until no pods match labelSelector, ctx is
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
