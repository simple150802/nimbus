package algorithm

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"nimbus/api/kubeapi"
	"nimbus/api/logging"
	"nimbus/api/nimbusevent"
)

// cMinWarmSamples is how many capped samples the warm c_min downward search
// takes per candidate CPU. Fewer than the full warmSamples because c_min only
// needs a coarse "is p95 above or below the SLO" verdict, not a precise plateau
// convergence — keeping the downward search cheap.
const cMinWarmSamples = 20

// getResptWarm measures warm steady-state inference latency at cpuValue.
//
// Flow:
//  1. Patch ksvc to cpuCold (= c_opt_cold from the cold phase) so the pod
//     initialises as fast as possible.
//  2. waitForScaleToZero — ensure the old pod is gone.
//  3. Re-assert cpuCold (guards against the online controller overwriting it
//     between waitForScaleToZero and the warmup trigger).
//  4. GET /status → wait "READY" — pod initialises at cpuCold, model loaded.
//     RT is discarded; we only care the pod is warm before we resize.
//  5. PatchPodCpu via resize subresource — in-place resize the running pod
//     to cpuValue without creating a new Knative revision or scale-to-zero.
//  6. sleep interWarmSampleSleep — let the kubelet propagate the new cgroup.
//  7. N timed GETs to the warm-gate URL → ProbeStats.
//
// This avoids StartupCPUBoost entirely. The boost controller polls the
// service URL asynchronously, but containerConcurrency=1 means the warmup
// request monopolises the pod for its full duration, blocking every poll.
// The pod then scales to zero before the controller can revert the CPU,
// making every boost-revert approach unreliable on this cluster.
func getResptWarm(ctx context.Context, event *nimbusevent.NimbusEvent, cpuValue string, cpuCold string, onSample SampleSink) (ProbeStats, error) {
	logging.Stage(fmt.Sprintf("[WARM] probe starting — cpuValue=%s cpuCold=%s ns=%s", cpuValue, cpuCold, event.Metadata.Namespace))
	labelSelector := buildLabelSelector(event)
	targetKsvc := event.Selector.MatchExpressions[0].Values[0]

	// Step 1: set ksvc to cpuCold so the pod initialises quickly.
	if _, err := kubeapi.PatchResourceLimits(ctx, event.Metadata.Namespace, targetKsvc, cpuCold); err != nil {
		logging.Failure("[WARM] failed to patch ksvc to cpuCold:", err)
		return ProbeStats{}, err
	}
	logging.Info(fmt.Sprintf("[WARM] ksvc set to cpuCold=%s for fast init; will resize to %s after READY", cpuCold, cpuValue))

	// Step 2: wait for old pod to be gone.
	if err := waitForScaleToZero(ctx, phaseWarm, event.Metadata.Namespace, labelSelector); err != nil {
		return ProbeStats{}, err
	}

	// Step 3: re-assert cpuCold — the online controller (StartController) runs
	// every 2s and calls ApplyKsvcSpec(c_opt_warm), which can overwrite the
	// ksvc while waitForScaleToZero is blocking.
	if _, err := kubeapi.PatchResourceLimits(ctx, event.Metadata.Namespace, targetKsvc, cpuCold); err != nil {
		logging.Failure("[WARM] failed to re-assert cpuCold after scale-to-zero:", err)
		return ProbeStats{}, err
	}

	monCtx, monCancel := context.WithCancel(ctx)
	defer monCancel()
	go kubeapi.MonitorKsvcResources(monCtx, phaseWarm, event.Metadata.Namespace, targetKsvc)

	// Step 4: trigger pod and wait for READY (model loaded at cpuCold). Discard RT.
	cold := event.Spec.DurationPolicy.ColdApiCondition
	statusURL := kubeapi.BuildKsvcStatusURL(event.Metadata.Namespace, targetKsvc, cold.Path)
	logging.Info(fmt.Sprintf("[WARM] waiting for READY at cpuCold=%s (discarded)", cpuCold))
	if _, err := triggerHttp(ctx, phaseWarm, statusURL, cold.Response); err != nil {
		return ProbeStats{}, err
	}

	// Step 5: in-place resize running pod to cpuValue via resize subresource.
	// No new revision, no scale-to-zero — kubelet applies new cgroup directly.
	if err := kubeapi.PatchPodCpu(ctx, event.Metadata.Namespace, labelSelector, cpuValue); err != nil {
		logging.Failure(fmt.Sprintf("[WARM] in-place resize to cpuValue=%s failed: %v", cpuValue, err))
		return ProbeStats{}, err
	}
	logging.Info(fmt.Sprintf("[WARM] pod resized in-place: %s → %s", cpuCold, cpuValue))

	// Step 6: wait for kubelet to apply new cgroup.
	logging.Normal(fmt.Sprintf("[WARM] cgroup settle sleep %s", interWarmSampleSleep))
	if err := sleepCtx(ctx, interWarmSampleSleep); err != nil {
		return ProbeStats{}, err
	}

	warm := event.Spec.DurationPolicy.WarmApiCondition
	targetURL := kubeapi.BuildKsvcStatusURL(event.Metadata.Namespace, targetKsvc, warm.Path)

	// Per-request timeout scaled to the warm SLO so minute-scale workloads
	// (e.g. LLM token generation) aren't cut off mid-response and retried forever.
	var sloWarm int64
	if event.Spec.AcceptableResponseTime != nil {
		sloWarm = event.Spec.AcceptableResponseTime.Warm
	}
	reqTimeout := warmRequestTimeout(sloWarm)

	// Step 6b: discard warmup call to flush background-goroutine backlog.
	// When the pod ran at cpuCold (75m) during init, Go runtime goroutines
	// were heavily throttled and accumulated pending work. After the in-place
	// resize to cpuValue the first hashChain call bears that backlog — other
	// goroutines rush to catch up and steal ~half the CFS quota, making
	// sample 1 systematically 2× slower. Firing one discarded /detect/local
	// call here absorbs the backlog so all timed samples run at steady speed.
	logging.Info("[WARM] warmup /detect/local (discard — flushes goroutine backlog from resize)")
	if _, err := triggerHttpWithCodeBody(ctx, phaseWarm, targetURL, warm.StatusCode, warm.BodyContains, reqTimeout); err != nil {
		return ProbeStats{}, err
	}
	logging.Normal(fmt.Sprintf("[WARM] post-warmup settle sleep %s", interWarmSampleSleep))
	if err := sleepCtx(ctx, interWarmSampleSleep); err != nil {
		return ProbeStats{}, err
	}

	// Step 7: timed samples at cpuValue.

	n := event.Spec.Measurement.WarmSamples
	if n < 1 {
		n = defaultWarmSamples
	}
	logging.Info(fmt.Sprintf("[WARM] samples to collect: %d at cpuValue=%s", n, cpuValue))

	samples := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		rt, err := triggerHttpWithCodeBody(ctx, phaseWarm, targetURL, warm.StatusCode, warm.BodyContains, reqTimeout)
		if err != nil {
			return ProbeStats{}, err
		}
		logging.Normal(fmt.Sprintf("[WARM] sample %d/%d: cpu=%s rt=%s", i+1, n, cpuValue, rt))
		if onSample != nil {
			onSample(rt)
		}
		samples = append(samples, rt)
		if i < n-1 {
			logging.Normal(fmt.Sprintf("[WARM] cool-down %s before next sample", interWarmSampleSleep))
			if err := sleepCtx(ctx, interWarmSampleSleep); err != nil {
				return ProbeStats{}, err
			}
		}
	}

	stats := computeProbeStats(samples)
	logging.Normal(fmt.Sprintf("[WARM] probe complete — cpu=%s avg=%s p90=%s p95=%s over %d samples",
		cpuValue, stats.Avg, stats.P90, stats.P95, n))
	return stats, nil
}

// warmReqCapped fires one warm-gate request under a hard deadline (cap). It
// returns (rt, overBudget, err): overBudget=true means the request did not
// finish within cap, i.e. the current CPU is too slow to serve a request inside
// the budget — the c_min search treats that as "fails SLO" and stops descending,
// instead of hanging on triggerHttp's infinite 90s-timeout retry loop. A
// parent-context cancellation (SIGINT) is propagated as a real error.
func warmReqCapped(ctx context.Context, url string, warm nimbusevent.WarmApiCondition, cap time.Duration) (time.Duration, bool, error) {
	cctx, cancel := context.WithTimeout(ctx, cap)
	defer cancel()
	// Per-request client timeout == cap: one request, bounded by the cap. A pod
	// too slow to answer within the cap times out and is reported overBudget,
	// instead of the client giving up early and the retry loop spinning.
	rt, err := triggerHttpWithCodeBody(cctx, phaseWarm, url, warm.StatusCode, warm.BodyContains, cap)
	if err != nil {
		if ctx.Err() != nil {
			return 0, false, ctx.Err() // parent cancelled — real abort
		}
		if cctx.Err() != nil {
			return cap, true, nil // our cap fired → over budget (too slow)
		}
		return 0, false, err
	}
	return rt, false, nil
}

// searchCMinWarm runs the dedicated downward search for the warm c_min over
// [minProbeCpuMilli, cpuBudget] — the low-CPU region the c_opt search never
// explores. It is efficient and hang-proof:
//
//   - One warm pod is brought up once (at cpuCold) and kept alive for the whole
//     search; each candidate CPU is applied by an in-place resize (no
//     scale-to-zero per point).
//   - Each request is capped at 2×SLO; a CPU too slow to answer within that is
//     declared "fails SLO" in seconds (vs the old searchCMin hanging forever at
//     the 50m floor on a CPU-bound workload).
//   - Only cMinWarmSamples (5) samples per candidate — enough to decide
//     above/below SLO.
//
// New probe points are appended to nr.WarmRtSamples (and exported) so they
// persist alongside the c_opt samples. Returns "" when sloWarm<=0 or when even
// cpuBudget fails the SLO (infeasible).
func searchCMinWarm(ctx context.Context, current *nimbusevent.NimbusEvent, node, cpuCold string, sloWarm int64) (string, error) {
	if sloWarm <= 0 {
		return "", nil
	}
	ns := current.Metadata.Namespace
	targetKsvc := current.Selector.MatchExpressions[0].Values[0]
	labelSelector := buildLabelSelector(current)
	metric := current.Spec.Metric
	gate := metricGate(metric)
	cpuBudget := current.Spec.ResourcePolicy.ContainerPolicies[0].CpuBudget
	nr := current.PerNodeResults[node]

	// Memo seeded from the c_opt search's samples so already-measured points
	// (notably cpuBudget) are never re-probed.
	memo := make(map[string]int64, len(nr.WarmRtSamples))
	for _, s := range nr.WarmRtSamples {
		memo[s.Cpu] = gateRtFromStats(&nimbusevent.RtStats{
			AvgMillis: s.RtMillis, P90Millis: s.RtP90Millis, P95Millis: s.RtP95Millis,
		}, metric)
	}

	// Feasibility: cpuBudget has the lowest RT; if it fails the SLO, nothing meets it.
	if v, ok := memo[cpuBudget]; ok && v > sloWarm {
		logging.Warning(fmt.Sprintf("[cmin] cpuBudget=%s p95=%dms > slo=%dms — warm SLO infeasible, c_min empty", cpuBudget, v, sloWarm))
		return "", nil
	}

	// Seed the search bracket from the points the c_opt search ALREADY probed
	// (free — no new measurement):
	//   hi     = smallest probed CPU meeting the SLO. This is ≤ c_opt and much
	//            tighter than cpuBudget. (We can't just use c_opt itself: when
	//            the SLO is tight the knee's RT can sit ~10% above cpuBudget's,
	//            so c_opt may FAIL the SLO and c_min would then live ABOVE it.
	//            Using the smallest KNOWN-meeting point is both tighter and safe.)
	//   loSeed = largest probed CPU failing the SLO and below hi.
	// If a failing probed point already brackets hi within the convergence
	// threshold, c_min is pinned — return now: no pod, no new probes.
	milliOf := func(cpu string) (int64, bool) {
		q, err := resource.ParseQuantity(cpu)
		if err != nil {
			return 0, false
		}
		return q.MilliValue(), true
	}
	hi := cpuBudget
	hiMilli, _ := milliOf(cpuBudget)
	for cpu, ms := range memo {
		if m, ok := milliOf(cpu); ok && ms <= sloWarm && m < hiMilli {
			hi, hiMilli = cpu, m
		}
	}
	loSeed := ""
	loMilli := int64(-1)
	for cpu, ms := range memo {
		if m, ok := milliOf(cpu); ok && ms > sloWarm && m < hiMilli && m > loMilli {
			loSeed, loMilli = cpu, m
		}
	}
	if loSeed != "" {
		cont, err := kubeapi.IsDiffGreaterThresh(loSeed, hi, convergenceThresholdMilli)
		if err != nil {
			return "", err
		}
		if !cont {
			logging.Success(fmt.Sprintf("[cmin] bracket already tight from probed points (lo=%s hi=%s) → c_min=%s, no new probes", loSeed, hi, hi))
			return hi, nil
		}
	}

	// Bring up one warm pod at cpuCold (fast init), kept alive for the whole search.
	if _, err := kubeapi.PatchResourceLimits(ctx, ns, targetKsvc, cpuCold); err != nil {
		return "", err
	}
	if err := waitForScaleToZero(ctx, phaseWarm, ns, labelSelector); err != nil {
		return "", err
	}
	if _, err := kubeapi.PatchResourceLimits(ctx, ns, targetKsvc, cpuCold); err != nil {
		return "", err
	}
	monCtx, monCancel := context.WithCancel(ctx)
	defer monCancel()
	go kubeapi.MonitorKsvcResources(monCtx, phaseWarm, ns, targetKsvc)

	cold := current.Spec.DurationPolicy.ColdApiCondition
	statusURL := kubeapi.BuildKsvcStatusURL(ns, targetKsvc, cold.Path)
	logging.Info(fmt.Sprintf("[cmin] bringing up warm pod at cpuCold=%s for downward search", cpuCold))
	if _, err := triggerHttp(ctx, phaseWarm, statusURL, cold.Response); err != nil {
		return "", err
	}

	warm := current.Spec.DurationPolicy.WarmApiCondition
	targetURL := kubeapi.BuildKsvcStatusURL(ns, targetKsvc, warm.Path)
	reqCap := time.Duration(2*sloWarm) * time.Millisecond

	// measure resizes the live pod to cpu, takes up to cMinWarmSamples capped
	// requests, and returns the gate-metric milliseconds (or sloWarm+1 if any
	// request blows the cap). Memoised; appends points to nr.WarmRtSamples.
	measure := func(cpu string) (int64, error) {
		if v, ok := memo[cpu]; ok {
			logging.Info(fmt.Sprintf("[cmin] cache hit cpu=%s=%dms", cpu, v))
			return v, nil
		}
		if err := kubeapi.PatchPodCpu(ctx, ns, labelSelector, cpu); err != nil {
			return 0, err
		}
		logging.Info(fmt.Sprintf("[cmin] pod resized to cpu=%s — measuring (cap=%s)", cpu, reqCap))
		if err := sleepCtx(ctx, interWarmSampleSleep); err != nil {
			return 0, err
		}
		// warmup discard (flush goroutine backlog after resize)
		if _, over, err := warmReqCapped(ctx, targetURL, warm, reqCap); err != nil {
			return 0, err
		} else if over {
			logging.Warning(fmt.Sprintf("[cmin] cpu=%s warmup exceeded cap %s → FAILS SLO", cpu, reqCap))
			memo[cpu] = sloWarm + 1
			appendCMinSample(current, node, cpu, ProbeStats{Avg: reqCap, P90: reqCap, P95: reqCap})
			return sloWarm + 1, nil
		}
		if err := sleepCtx(ctx, interWarmSampleSleep); err != nil {
			return 0, err
		}

		sink := makeSampleSink(current, node, "warm", cpu)
		ss := make([]time.Duration, 0, cMinWarmSamples)
		for i := 0; i < cMinWarmSamples; i++ {
			rt, over, err := warmReqCapped(ctx, targetURL, warm, reqCap)
			if err != nil {
				return 0, err
			}
			if over {
				logging.Warning(fmt.Sprintf("[cmin] cpu=%s sample %d exceeded cap %s → FAILS SLO", cpu, i+1, reqCap))
				memo[cpu] = sloWarm + 1
				appendCMinSample(current, node, cpu, ProbeStats{Avg: reqCap, P90: reqCap, P95: reqCap})
				return sloWarm + 1, nil
			}
			if sink != nil {
				sink(rt)
			}
			ss = append(ss, rt)
			if i < cMinWarmSamples-1 {
				if err := sleepCtx(ctx, interWarmSampleSleep); err != nil {
					return 0, err
				}
			}
		}
		stats := computeProbeStats(ss)
		appendCMinSample(current, node, cpu, stats)
		v := gate(stats).Milliseconds()
		memo[cpu] = v
		logging.Info(fmt.Sprintf("[cmin] cpu=%s p95=%dms (%d capped samples)", cpu, v, cMinWarmSamples))
		return v, nil
	}

	// Set the lower bound. If the c_opt samples already gave a failing point
	// below hi, use it (no floor probe needed). Otherwise the SLO crossing may
	// lie below every probed point — anchor at the safety floor and confirm the
	// floor itself fails (if even the floor meets the SLO, c_min = floor).
	var lo string
	if loSeed != "" {
		lo = loSeed
	} else {
		floorCpu := fmt.Sprintf("%dm", minProbeCpuMilli)
		floorMs, err := measure(floorCpu)
		if err != nil {
			return "", err
		}
		if floorMs <= sloWarm {
			logging.Success(fmt.Sprintf("[cmin] warm: safety floor %s already meets SLO → c_min=%s", floorCpu, floorCpu))
			return floorCpu, nil
		}
		lo = floorCpu
	}
	logging.Info(fmt.Sprintf("[cmin] warm downward search bracket: lo=%s hi=%s", lo, hi))

	// Invariant: lo = largest CPU known NOT to meet SLO; hi = smallest known to meet.
	for {
		cont, err := kubeapi.IsDiffGreaterThresh(lo, hi, convergenceThresholdMilli)
		if err != nil {
			return "", err
		}
		if !cont {
			break
		}
		mid, err := kubeapi.CalculateAverageCPU(lo, hi)
		if err != nil {
			return "", err
		}
		mq, err := resource.ParseQuantity(mid)
		if err != nil {
			return "", err
		}
		if mq.MilliValue() < minProbeCpuMilli {
			break
		}
		ms, err := measure(mid)
		if err != nil {
			return "", err
		}
		if ms <= sloWarm {
			hi = mid // meets — go lower
		} else {
			lo = mid // fails — go higher
		}
	}
	logging.Success(fmt.Sprintf("[cmin] warm downward search complete: c_min=%s (slo=%dms)", hi, sloWarm))
	return hi, nil
}

// appendCMinSample records one aggregated (cpu, stats) point from the warm
// c_min search into nr.WarmRtSamples so it persists in status and feeds
// sortSamplesByCpu. Mirrors the recordSample closure used by the c_opt search.
func appendCMinSample(current *nimbusevent.NimbusEvent, node, cpu string, stats ProbeStats) {
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
