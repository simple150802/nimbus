package algorithm

import (
	"context"
	"fmt"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"nimbus/api/kubeapi"
	"nimbus/api/logging"
	"nimbus/api/nimbusevent"
	"nimbus/internal/export"
)

const (
	// convergenceThresholdMilli stops the binary search when high - low
	// drops below this threshold (milli-CPU). 100m corresponds to roughly
	// the granularity Kubernetes' scheduler can act on.
	convergenceThresholdMilli = 100

	// responseTimeImprovementGate is the relative drop in measured
	// response time needed to justify moving the lower bound up rather
	// than narrowing toward the upper bound. 0.10 == 10%.
	responseTimeImprovementGate = 0.10

	// floorResetCpu is the CPU value the ksvc spec is reset to before
	// each per-node binary search. The upstream kube-startup-cpu-boost
	// webhook only RAISES CPU at pod creation; if the ksvc carries a
	// residual limit from a previous run, every probe at a target below
	// that value is silently skipped. Patching to a value strictly below
	// minProbeCpuMilli guarantees every in-range probe satisfies the
	// webhook's "target >= current" gate. 10m is safe — the search will
	// never target this low (the safety floor catches us at
	// minProbeCpuMilli).
	floorResetCpu = "10m"
)

// BinarySearch runs both phases of the CPU search for a single candidate
// node. The caller is expected to have pinned the ksvc to that node before
// calling (so every probed pod lands there) and to unpin afterwards.
// Convergence values are written into current.PerNodeResults[node] — the
// entry is created if missing.
func BinarySearch(ctx context.Context, current *nimbusevent.NimbusEvent, node string) (string, error) {
	ns := current.Metadata.Namespace
	ksvc := current.Selector.MatchExpressions[0].Values[0]

	if current.PerNodeResults == nil {
		current.PerNodeResults = make(map[string]*nimbusevent.NodeResult)
	}
	if current.PerNodeResults[node] == nil {
		current.PerNodeResults[node] = &nimbusevent.NodeResult{}
	}

	// Reset the ksvc's CPU limit before probing. The upstream
	// kube-startup-cpu-boost webhook only RAISES CPU at pod creation —
	// without a low residual value, in-range probes can be silently
	// skipped. floorResetCpu is intentionally below minProbeCpuMilli so
	// every probe in the active search satisfies the webhook's
	// "target >= current" gate. Cannot just remove limits — the boost
	// project requires an existing limit to snapshot for revert.
	if err := kubeapi.PatchResourceLimits(ctx, ns, ksvc, floorResetCpu); err != nil {
		return "", fmt.Errorf("failed to reset ksvc cpu before binary search: %w", err)
	}

	// Pin the ksvc to one pod for the whole search — both the starting and
	// running phases need deterministic measurement, so neither should share
	// traffic across multiple pods. Cleared on any exit path via defer so the
	// cap never outlives the search.
	if err := kubeapi.PatchMaxScale(ctx, ns, ksvc); err != nil {
		return "", fmt.Errorf("failed to set maxScale=1 before binary search: %w", err)
	}
	defer func() {
		if err := kubeapi.UnsetMaxScale(ctx, ns, ksvc); err != nil {
			logging.Warning("failed to unset maxScale after binary search:", err)
		}
	}()

	if _, err := binarySearchForStartingPhase(ctx, current, node); err != nil {
		return "", fmt.Errorf("starting phase aborted: %w", err)
	}
	if _, err := binarySearchForRunningPhase(ctx, current, node); err != nil {
		return "", fmt.Errorf("running phase aborted: %w", err)
	}

	result := current.PerNodeResults[node]
	logging.Info(fmt.Sprintf("[node=%s] cold c_opt=%s c_min=%s | warm c_opt=%s c_min=%s",
		node, result.StartingCpu, result.CMinStarting, result.RunningCpu, result.CMinRunning))

	return result.RunningCpu, nil
}

// probeFn is a phase-agnostic measurement primitive. The starting phase
// passes a closure around getResptCold; the running phase passes one
// around getResptWarm. Both return ProbeStats (avg + p90 + p95) for one
// probe-point's batch of N samples. The convergence math uses whichever
// percentile metricGate(current.Spec.Metric) selects; the other two
// ride along for the recordSample callback to persist.
type probeFn func(ctx context.Context, current *nimbusevent.NimbusEvent, cpu string) (ProbeStats, error)

// runBinarySearch is the shared per-phase search loop. It bisects over
// [low=0, high=cpuBudget] until the bracket falls below
// convergenceThresholdMilli (resolution stop) OR the next midpoint
// would fall below minProbeCpuMilli (safety floor stop). The invariant
// throughout: `high` is the smallest CPU known to be ON the plateau,
// `low` is the largest CPU known to be OFF the plateau (or 0 if the
// plateau hasn't been left yet). c_opt = high at exit.
//
// Before the loop, runBinarySearch probes at cpuBudget once as a
// feasibility check — if the gate metric there exceeds sloRtMillis,
// it logs a warning but doesn't abort (c_opt is still a useful
// number; c_min for this phase will end up "" via deriveMin since
// no sample meets the SLO).
//
// recordSample is invoked once per UNIQUE probed CPU (cache hits don't
// re-record). The caller's recordSample appends into the active node's
// ColdRtSamples / WarmRtSamples slice — runBinarySearch stays
// storage-agnostic.
//
// setOpt is invoked once at end-of-loop with the converged c_opt CPU.
// The caller uses it to write nr.StartingCpu / nr.RunningCpu.
//
// `sloRtMillis = 0` means "no SLO budget for this phase"
// (spec.acceptableResponseTime.<phase> absent) — the feasibility-check
// warning is suppressed and c_min derivation by the caller will return
// "" via deriveMin.
func runBinarySearch(
	ctx context.Context,
	current *nimbusevent.NimbusEvent,
	probe probeFn,
	cpuBudget string,
	sloRtMillis int64,
	setOpt func(cpu string),
	recordSample func(cpu string, stats ProbeStats),
) (string, error) {
	gate := metricGate(current.Spec.Metric)
	logging.Info(fmt.Sprintf("runBinarySearch gating on metric=%s, cpuBudget=%s, slo=%dms",
		resolvedMetric(current.Spec.Metric), cpuBudget, sloRtMillis))

	// probeOnce memoizes probe() by CPU for the lifetime of this
	// runBinarySearch call. Per phase. Repeated probes at the same CPU
	// (most importantly the high-side every iteration of bisect) are
	// free after the first one. recordSample is folded inside so cache
	// hits don't duplicate sample rows — the sample list ends up with
	// exactly one entry per unique probed CPU.
	cache := make(map[string]ProbeStats)
	probeOnce := func(cpu string) (ProbeStats, error) {
		if stats, ok := cache[cpu]; ok {
			logging.Info(fmt.Sprintf("[search] cache hit cpu=%s — skipping re-probe", cpu))
			return stats, nil
		}
		stats, err := probe(ctx, current, cpu)
		if err != nil {
			return ProbeStats{}, err
		}
		cache[cpu] = stats
		recordSample(cpu, stats)
		return stats, nil
	}

	// Step 1 — feasibility check at the ceiling. Informational only;
	// don't abort the search. c_min derivation by the caller signals
	// SLO infeasibility via empty-string return.
	statsBudget, err := probeOnce(cpuBudget)
	if err != nil {
		return "", err
	}
	if sloRtMillis > 0 && gate(statsBudget).Milliseconds() > sloRtMillis {
		logging.Warning(fmt.Sprintf(
			"SLO unachievable at cpuBudget=%s — gate=%dms > slo=%dms; c_min for this phase will be empty",
			cpuBudget, gate(statsBudget).Milliseconds(), sloRtMillis,
		))
	}

	// Step 2 — unified bisect over [low=0, high=cpuBudget].
	low := "0"
	high := cpuBudget

	for {
		// Resolution stop — gap is fine enough; high is our answer.
		shouldContinue, err := kubeapi.IsDiffGreaterThresh(low, high, convergenceThresholdMilli)
		if err != nil {
			return "", fmt.Errorf("calculating bracket threshold: %w", err)
		}
		if !shouldContinue {
			break
		}

		midCpu, err := kubeapi.CalculateAverageCPU(low, high)
		if err != nil {
			return "", fmt.Errorf("calculating mid: %w", err)
		}

		// Safety-floor stop — refuse to probe at potentially-crash CPU.
		midQty, err := resource.ParseQuantity(midCpu)
		if err != nil {
			return "", fmt.Errorf("parsing mid %q: %w", midCpu, err)
		}
		if midQty.MilliValue() < minProbeCpuMilli {
			logging.Warning(fmt.Sprintf(
				"runBinarySearch safety floor hit: midpoint %s < %dm — stopping with c_opt=%s",
				midCpu, minProbeCpuMilli, high,
			))
			break
		}

		logging.Info("Checking at " + midCpu + " CPU ...")

		statsMid, err := probeOnce(midCpu)
		if err != nil {
			return "", err
		}
		rtMid := gate(statsMid)

		// rtHigh comes from the cache after the first iteration (high
		// only narrows when we already probed it as a previous mid).
		statsHigh, err := probeOnce(high)
		if err != nil {
			return "", err
		}
		rtHigh := gate(statsHigh)

		improvement := float64(rtMid-rtHigh) / float64(rtMid)
		if improvement >= responseTimeImprovementGate {
			// mid is meaningfully worse than high → mid is OFF plateau.
			// c_opt is in (mid, high]; move low up to mid.
			low = midCpu
		} else {
			// mid is ~equal to high → mid is ON plateau.
			// c_opt ≤ mid; narrow high down to mid.
			high = midCpu
		}
	}

	logging.Success(fmt.Sprintf("runBinarySearch complete: c_opt=%s", high))
	setOpt(high)
	return high, nil
}

// sortSamplesByCpu canonicalizes a sample list so the persisted order
// matches what the online stage's piecewise-linear interpolator and the
// deriveMin walk both expect (ascending cpu). Called once at end-of-phase,
// after all samples for a (node, phase) tuple have been collected.
func sortSamplesByCpu(samples []nimbusevent.SamplePoint) {
	sort.Slice(samples, func(i, j int) bool {
		qi, errI := resource.ParseQuantity(samples[i].Cpu)
		qj, errJ := resource.ParseQuantity(samples[j].Cpu)
		// Unparseable strings sort to the end deterministically; should
		// never happen since CalculateAverageCPU produces valid quantities.
		if errI != nil || errJ != nil {
			return errI == nil && errJ != nil
		}
		return qi.MilliValue() < qj.MilliValue()
	})
}

// makeSampleSink returns a SampleSink that appends one CSV row per call
// to <ExportRoot>/<node>/<phase>/<cpu>.csv. Returns nil when ExportRoot is
// empty (export disabled), so getResptCold/getResptWarm skip the call.
// The full time.Duration is forwarded so AppendSample can write float ms
// without truncating to int.
func makeSampleSink(current *nimbusevent.NimbusEvent, node, phase, cpu string) SampleSink {
	if current.ExportRoot == "" {
		return nil
	}
	return func(rt time.Duration) {
		if err := export.AppendSample(current.ExportRoot, node, phase, cpu, rt); err != nil {
			logging.Warning(fmt.Sprintf("[export] AppendSample failed (%s %s %s): %v", node, phase, cpu, err))
		}
	}
}

// latestSampleAt returns the most-recently-recorded SamplePoint at the
// given cpu in the supplied list, or nil if none exists. With probeOnce
// caching there should be exactly one entry per CPU, but the linear
// scan stays robust against pre-cache call sites.
func latestSampleAt(samples []nimbusevent.SamplePoint, cpu string) *nimbusevent.SamplePoint {
	for i := len(samples) - 1; i >= 0; i-- {
		if samples[i].Cpu == cpu {
			return &samples[i]
		}
	}
	return nil
}

// rtStatsFromSample copies a SamplePoint's three percentile fields into
// an RtStats struct. Returns nil if s is nil.
func rtStatsFromSample(s *nimbusevent.SamplePoint) *nimbusevent.RtStats {
	if s == nil {
		return nil
	}
	return &nimbusevent.RtStats{
		AvgMillis: s.RtMillis,
		P90Millis: s.RtP90Millis,
		P95Millis: s.RtP95Millis,
	}
}

func binarySearchForStartingPhase(ctx context.Context, current *nimbusevent.NimbusEvent, node string) (string, error) {
	cpuBudget := current.Spec.ResourcePolicy.ContainerPolicies[0].CpuBudget

	var sloCold int64
	if current.Spec.AcceptableResponseTime != nil {
		sloCold = current.Spec.AcceptableResponseTime.Cold
	}

	probe := func(ctx context.Context, ev *nimbusevent.NimbusEvent, cpu string) (ProbeStats, error) {
		return getResptCold(ctx, ev, cpu, makeSampleSink(ev, node, "cold", cpu))
	}
	setOpt := func(cpu string) {
		nr := current.PerNodeResults[node]
		nr.StartingCpu = cpu
		nr.StartingSaturated = true
		nr.StartingRt = rtStatsFromSample(latestSampleAt(nr.ColdRtSamples, cpu))
	}
	recordSample := func(cpu string, stats ProbeStats) {
		current.PerNodeResults[node].ColdRtSamples = append(
			current.PerNodeResults[node].ColdRtSamples,
			nimbusevent.SamplePoint{
				Cpu:         cpu,
				RtMillis:    stats.Avg.Milliseconds(),
				RtP90Millis: stats.P90.Milliseconds(),
				RtP95Millis: stats.P95.Milliseconds(),
			},
		)
	}

	cOpt, err := runBinarySearch(ctx, current, probe, cpuBudget, sloCold, setOpt, recordSample)
	if err != nil {
		return "", err
	}

	// End-of-phase: sort samples ascending by CPU (required by deriveMin
	// and by online-stage interpolation), then derive c_min from the
	// sorted curve.
	nr := current.PerNodeResults[node]
	sortSamplesByCpu(nr.ColdRtSamples)
	nr.CMinStarting = deriveMin(nr.ColdRtSamples, current.Spec.Metric, sloCold)
	if sloCold > 0 && nr.CMinStarting == "" {
		logging.Warning(fmt.Sprintf(
			"node=%s cold phase: no probed CPU met SLO=%dms — c_min_cold left empty",
			node, sloCold,
		))
	}

	return cOpt, nil
}

func binarySearchForRunningPhase(ctx context.Context, current *nimbusevent.NimbusEvent, node string) (string, error) {
	cpuBudget := current.Spec.ResourcePolicy.ContainerPolicies[0].CpuBudget

	var sloWarm int64
	if current.Spec.AcceptableResponseTime != nil {
		sloWarm = current.Spec.AcceptableResponseTime.Warm
	}

	probe := func(ctx context.Context, ev *nimbusevent.NimbusEvent, cpu string) (ProbeStats, error) {
		return getResptWarm(ctx, ev, cpu, cpuBudget, makeSampleSink(ev, node, "warm", cpu))
	}
	setOpt := func(cpu string) {
		nr := current.PerNodeResults[node]
		nr.RunningCpu = cpu
		nr.RunningSaturated = true
		nr.RunningRt = rtStatsFromSample(latestSampleAt(nr.WarmRtSamples, cpu))
	}
	recordSample := func(cpu string, stats ProbeStats) {
		current.PerNodeResults[node].WarmRtSamples = append(
			current.PerNodeResults[node].WarmRtSamples,
			nimbusevent.SamplePoint{
				Cpu:         cpu,
				RtMillis:    stats.Avg.Milliseconds(),
				RtP90Millis: stats.P90.Milliseconds(),
				RtP95Millis: stats.P95.Milliseconds(),
			},
		)
	}

	cOpt, err := runBinarySearch(ctx, current, probe, cpuBudget, sloWarm, setOpt, recordSample)
	if err != nil {
		return "", err
	}

	nr := current.PerNodeResults[node]
	sortSamplesByCpu(nr.WarmRtSamples)
	nr.CMinRunning = deriveMin(nr.WarmRtSamples, current.Spec.Metric, sloWarm)
	if sloWarm > 0 && nr.CMinRunning == "" {
		logging.Warning(fmt.Sprintf(
			"node=%s warm phase: no probed CPU met SLO=%dms — c_min_warm left empty",
			node, sloWarm,
		))
	}

	return cOpt, nil
}
