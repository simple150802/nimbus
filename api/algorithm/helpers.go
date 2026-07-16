package algorithm

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"nimbus/api/kubeconfig"
	"nimbus/api/logging"
	"nimbus/api/nimbusevent"
)

// ProbeStats summarises a probe's individual-sample response times.
// Avg is the arithmetic mean; P90 / P95 are nearest-rank percentiles.
// One of these three drives binary-search convergence per run — chosen
// via NimbusSpec.Metric and resolved at call sites through metricGate.
// Computed by computeProbeStats from the per-sample slice held inside
// getResptCold / getResptWarm — never escapes the probe function.
type ProbeStats struct {
	Avg time.Duration
	P90 time.Duration
	P95 time.Duration
}

// MetricAvg / MetricP90 / MetricP95 are the legal values of
// NimbusSpec.Metric. Mirror the CRD enum in config/crd.yaml. Empty
// string falls through to MetricP95 inside metricGate.
const (
	MetricAvg = "avg"
	MetricP90 = "p90"
	MetricP95 = "p95"
)

// metricGate returns the ProbeStats accessor the binary-search
// convergence math should use for this run. Unknown / empty values
// default to p95 — same default as the CRD — so an upgraded
// controller running against an old Nimbus that predates the field
// behaves identically to a fresh one with no spec.metric set.
func metricGate(metric string) func(ProbeStats) time.Duration {
	switch metric {
	case MetricAvg:
		return func(s ProbeStats) time.Duration { return s.Avg }
	case MetricP90:
		return func(s ProbeStats) time.Duration { return s.P90 }
	case MetricP95, "":
		return func(s ProbeStats) time.Duration { return s.P95 }
	default:
		logging.Warning(fmt.Sprintf("unknown spec.metric=%q, falling back to p95", metric))
		return func(s ProbeStats) time.Duration { return s.P95 }
	}
}

// resolvedMetric returns the metric name actually used by metricGate for
// the given spec value. Mirrors metricGate's fallback rules so logging
// and meta consumers can report the effective gate without re-deriving
// the switch.
func resolvedMetric(metric string) string {
	switch metric {
	case MetricAvg, MetricP90, MetricP95:
		return metric
	default:
		return MetricP95
	}
}

// computeProbeStats returns the mean and nearest-rank p90 / p95 over a
// non-empty sample list. samples is left untouched (the caller's slice
// order is preserved); the function takes a defensive copy before
// sorting. Defined as a free helper so probe_cold and probe_warm share
// one implementation.
//
// Nearest-rank percentile: rank = ceil(q · N), 1-based.
//   N=3,  q=0.9  → rank 3 → samples[2]
//   N=3,  q=0.95 → rank 3 → samples[2]
//   N=10, q=0.9  → rank 9 → samples[8]
//   N=10, q=0.95 → rank 10 → samples[9]
// For N=1, P90 = P95 = the single sample.
func computeProbeStats(samples []time.Duration) ProbeStats {
	if len(samples) == 0 {
		return ProbeStats{}
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum time.Duration
	for _, s := range samples {
		sum += s
	}

	n := float64(len(sorted))
	idx90 := int(math.Ceil(0.90*n)) - 1
	idx95 := int(math.Ceil(0.95*n)) - 1
	if idx90 < 0 {
		idx90 = 0
	}
	if idx95 < 0 {
		idx95 = 0
	}

	return ProbeStats{
		Avg: sum / time.Duration(len(samples)),
		P90: sorted[idx90],
		P95: sorted[idx95],
	}
}

// deriveMin returns c_min for a phase: the smallest CPU in `samples` at
// which the gate metric (selected by `metric`) is at or below
// `sloRtMillis`. Returns "" when no sample meets the budget — that's the
// correct signal for "this node can't satisfy the SLO at any probed
// CPU." Caller logs a warning and writes "" into status.perNode.*.
//
// `samples` is assumed sorted ascending by CPU (the binary-search
// wrappers call sortSamplesByCpu at end-of-phase). If a caller passes
// an unsorted slice, this function is still correct — it just walks
// every entry and returns the first match — but performance is O(N)
// rather than the same O(N) it would be on a sorted list (no asymptotic
// difference; the comment is here to flag the design intent for future
// optimisations that might rely on sortedness, e.g. early exit).
//
// `sloRtMillis = 0` is a sentinel meaning "no budget for this phase"
// (spec.acceptableResponseTime.<phase> absent). Returns "" immediately
// without walking the list — c_min derivation is skipped for this phase.
func deriveMin(samples []nimbusevent.SamplePoint, metric string, sloRtMillis int64) string {
	if sloRtMillis <= 0 || len(samples) == 0 {
		return ""
	}
	pick := func(s nimbusevent.SamplePoint) int64 {
		switch metric {
		case MetricAvg:
			return s.RtMillis
		case MetricP90:
			return s.RtP90Millis
		case MetricP95, "":
			return s.RtP95Millis
		}
		return s.RtP95Millis
	}
	for _, s := range samples {
		if pick(s) <= sloRtMillis {
			return s.Cpu
		}
	}
	return ""
}

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

	// interColdSampleSleep is the cool-down between cold-phase samples.
	// Each cold sample force-deletes the pod and waits for scale-to-zero;
	// this delay then lets the node-level container teardown and Knative
	// endpoint routing fully propagate before the next trigger. Without it
	// the next curl can land on a still-terminating pod and record a bogus
	// sub-second "cold start". 10 s empirically covers kubelet grace +
	// Knative endpoint propagation.
	interColdSampleSleep = 10 * time.Second

	// interWarmSampleSleep is the cool-down between warm-phase timed samples.
	// The pod is already up (the warmup curl brought it up before the timed
	// loop), so no endpoint propagation is needed. The only requirement is
	// that the gap stays well below the ksvc's autoscaling window so KPA
	// does not scale the pod to zero between samples and turn "warm"
	// measurements into cold starts. 2 s is enough for queue-proxy to drain
	// the previous request at containerConcurrency=1 while staying far
	// shorter than a typical autoscaling window (≥ 10 s).
	interWarmSampleSleep = 2 * time.Second

	phaseCold = "COLD"
	phaseWarm = "WARM"

	// minProbeCpuMilli is the safety floor for binary-search probes.
	// runBinarySearch refuses to probe at a CPU below this value — some
	// workloads crash-loop at very small allocations and stuck-recovery
	// would burn the maxStuckRetries budget for no gain. When the next
	// bisect midpoint would fall below this floor, runBinarySearch exits
	// the loop and returns the current `high` as c_opt.
	minProbeCpuMilli = 50
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
