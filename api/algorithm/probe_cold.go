package algorithm

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/resource"

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

	// cMinColdSamples is the per-point sample count for the cold c_min
	// downward search. 20 is the minimum that lets p95 (nearest-rank) ignore
	// exactly one unlucky slow sample: rank = ceil(0.95×20) = 19, so p95 is the
	// 2nd-highest of 20 and one outlier alone can't reject an otherwise-good CPU.
	// Failing points early-exit (see searchCMinCold) so only PASS points pay the
	// full 20 cold-starts.
	cMinColdSamples = 20
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
		logging.Normal(fmt.Sprintf("[COLD] cool-down %s for endpoint propagation", interColdSampleSleep))
		if err := sleepCtx(ctx, interColdSampleSleep); err != nil {
			return 0, err
		}

		// Post-sleep guard: the kube-startup-cpu-boost controller polls the
		// ksvc's service URL while the StartupCPUBoost CR is active. That
		// poll goes through the Knative activator and can trigger a spurious
		// scale-from-zero during the sleep window. If the model is in the OS
		// page cache the pod can become READY before our trigger fires,
		// producing a bogus ~7ms "cold start". Re-check for pods and evict
		// any that appeared so the trigger always hits a truly cold pod.
		existing, err := kubeapi.DeleteKsvcPods(ctx, event.Metadata.Namespace, labelSelector)
		if err != nil {
			return 0, err
		}
		if len(existing) > 0 {
			logging.Warning(fmt.Sprintf("[COLD] sample %d/%d: %d pod(s) appeared during cool-down (boost-controller poll race) — evicting and re-waiting",
				sampleIdx, sampleTotal, len(existing)))
			if err := waitForScaleToZero(ctx, phaseCold, event.Metadata.Namespace, labelSelector); err != nil {
				return 0, err
			}
			if err := sleepCtx(ctx, interColdSampleSleep); err != nil {
				return 0, err
			}
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

// coldSampleCapped runs ONE cold-start sample with a hard deadline (cap), used
// by the cold c_min downward search. Unlike coldSampleWithStuckRecovery it does
// NOT retry-on-stuck — instead, a cold start that exceeds the cap is reported as
// over=true (RT clamped to cap). The c_min search treats an over-cap sample as
// "exceeds SLO", so an unusably-slow low-CPU point is detected in ~cap seconds
// rather than burning the stuck-retry budget. Caller must have set the boost CR
// to the target CPU so the fresh pod cold-starts at it.
func coldSampleCapped(ctx context.Context, event *nimbusevent.NimbusEvent, labelSelector, targetKsvc, targetURL string, reqCap time.Duration) (time.Duration, bool, error) {
	ns := event.Metadata.Namespace
	if _, err := kubeapi.DeleteKsvcPods(ctx, ns, labelSelector); err != nil {
		return 0, false, err
	}
	if err := waitForScaleToZero(ctx, phaseCold, ns, labelSelector); err != nil {
		return 0, false, err
	}
	if err := sleepCtx(ctx, interColdSampleSleep); err != nil {
		return 0, false, err
	}
	// Post-sleep guard: evict any pod the boost-controller poll race spun up
	// during the cool-down (it can become READY before our trigger → bogus ~8ms).
	if existing, err := kubeapi.DeleteKsvcPods(ctx, ns, labelSelector); err != nil {
		return 0, false, err
	} else if len(existing) > 0 {
		if err := waitForScaleToZero(ctx, phaseCold, ns, labelSelector); err != nil {
			return 0, false, err
		}
		if err := sleepCtx(ctx, interColdSampleSleep); err != nil {
			return 0, false, err
		}
	}

	monCtx, monCancel := context.WithCancel(ctx)
	go kubeapi.MonitorKsvcResources(monCtx, phaseCold, ns, targetKsvc)
	probeCtx, probeCancel := context.WithTimeout(ctx, reqCap)
	rt, err := triggerHttp(probeCtx, phaseCold, targetURL, event.Spec.DurationPolicy.ColdApiCondition.Response)
	probeCancel()
	monCancel()

	if err != nil {
		if ctx.Err() != nil {
			return 0, false, ctx.Err() // parent cancelled — real abort
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return reqCap, true, nil // exceeded cap → over SLO
		}
		return 0, false, err
	}
	return rt, false, nil
}

// allowedOverSLO returns how many of k samples may exceed the SLO while the
// nearest-rank gate metric still passes. For p95 with k=20: rank=ceil(0.95·20)=19,
// so the gate is the 2nd-highest sample → exactly one over-SLO sample is allowed.
// Returns -1 for the avg metric (no per-sample early-exit; gate on the mean).
func allowedOverSLO(metric string, k int) int {
	switch resolvedMetric(metric) {
	case MetricAvg:
		return -1
	case MetricP90:
		return k - int(math.Ceil(0.90*float64(k)))
	default: // p95 (and the unknown/empty fallback)
		return k - int(math.Ceil(0.95*float64(k)))
	}
}

// appendCMinColdSample records one aggregated (cpu, stats) point from the cold
// c_min search into nr.ColdRtSamples so it persists in status and feeds
// sortSamplesByCpu. Mirrors the recordSample closure used by the c_opt search.
func appendCMinColdSample(current *nimbusevent.NimbusEvent, node, cpu string, stats ProbeStats) {
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

// searchCMinCold runs the dedicated downward search for the cold c_min over
// [minProbeCpuMilli, cpuBudget] — the low-CPU region the c_opt knee search never
// explores. It cannot reuse warm's in-place-resize trick: a cold start needs a
// fresh pod, so every sample force-scales-to-zero (~80s). Three things keep the
// cost bounded:
//
//   - **Bracket from c_opt samples first**: if a probed FAILING point already
//     brackets the smallest probed PASSING point within the convergence
//     threshold, c_min is pinned — return with zero new cold starts. This is the
//     common tight-SLO case and degrades to the old deriveMin behaviour for free.
//   - **Per-sample cap (2×SLO)**: a cold start exceeding the cap counts as
//     over-SLO, so an unusably-slow low CPU is rejected in ~cap seconds.
//   - **Early-exit on the (allowedOver+1)-th over-SLO sample**: once enough
//     samples exceed the SLO that the gate metric must fail, stop — failing
//     points cost ~2 cold starts, only PASS points pay the full cMinColdSamples.
//
// Returns "" when sloCold<=0 or when even cpuBudget fails the SLO (infeasible).
func searchCMinCold(ctx context.Context, current *nimbusevent.NimbusEvent, node string, sloCold int64) (string, error) {
	if sloCold <= 0 {
		return "", nil
	}
	ns := current.Metadata.Namespace
	targetKsvc := current.Selector.MatchExpressions[0].Values[0]
	labelSelector := buildLabelSelector(current)
	boostName := current.Metadata.Name + "-" + targetKsvc
	metric := current.Spec.Metric
	gate := metricGate(metric)
	cpuBudget := current.Spec.ResourcePolicy.ContainerPolicies[0].CpuBudget
	targetURL := kubeapi.BuildKsvcStatusURL(ns, targetKsvc, current.Spec.DurationPolicy.ColdApiCondition.Path)
	nr := current.PerNodeResults[node]

	// Memo seeded from the c_opt search's samples (no re-probe of those points).
	memo := make(map[string]int64, len(nr.ColdRtSamples))
	for _, s := range nr.ColdRtSamples {
		memo[s.Cpu] = gateRtFromStats(&nimbusevent.RtStats{
			AvgMillis: s.RtMillis, P90Millis: s.RtP90Millis, P95Millis: s.RtP95Millis,
		}, metric)
	}

	// Feasibility: cpuBudget has the lowest RT; if it fails the SLO, nothing meets it.
	if v, ok := memo[cpuBudget]; ok && v > sloCold {
		logging.Warning(fmt.Sprintf("[cmin-cold] cpuBudget=%s gate=%dms > slo=%dms — cold SLO infeasible, c_min empty", cpuBudget, v, sloCold))
		return "", nil
	}

	// Seed the bracket from already-probed points (free).
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
		if m, ok := milliOf(cpu); ok && ms <= sloCold && m < hiMilli {
			hi, hiMilli = cpu, m
		}
	}
	loSeed := ""
	loMilli := int64(-1)
	for cpu, ms := range memo {
		if m, ok := milliOf(cpu); ok && ms > sloCold && m < hiMilli && m > loMilli {
			loSeed, loMilli = cpu, m
		}
	}
	if loSeed != "" {
		cont, err := kubeapi.IsDiffGreaterThresh(loSeed, hi, convergenceThresholdMilli)
		if err != nil {
			return "", err
		}
		if !cont {
			logging.Success(fmt.Sprintf("[cmin-cold] bracket already tight from probed points (lo=%s hi=%s) → c_min=%s, no new cold starts", loSeed, hi, hi))
			return hi, nil
		}
	}

	allowedOver := allowedOverSLO(metric, cMinColdSamples)
	reqCap := time.Duration(2*sloCold) * time.Millisecond

	// measure cold-starts cpu up to cMinColdSamples times (capped), with
	// early-exit once too many samples exceed the SLO. Sets the boost CR to cpu
	// so each fresh pod injects it. Memoised; appends a point to nr.ColdRtSamples.
	measure := func(cpu string) (int64, error) {
		if v, ok := memo[cpu]; ok {
			logging.Info(fmt.Sprintf("[cmin-cold] cache hit cpu=%s=%dms", cpu, v))
			return v, nil
		}
		kubeapi.CreateStartupCPUBoost(ctx, current, targetKsvc, cpu)
		defer kubeapi.DeleteStartupCPUBoost(ctx, ns, boostName)

		logging.Info(fmt.Sprintf("[cmin-cold] probing cpu=%s (up to %d cold starts, cap=%s, allowedOver=%d)", cpu, cMinColdSamples, reqCap, allowedOver))
		sink := makeSampleSink(current, node, "cold", cpu)
		samples := make([]time.Duration, 0, cMinColdSamples)
		f := 0
		var sumMs int64
		for i := 0; i < cMinColdSamples; i++ {
			rt, over, err := coldSampleCapped(ctx, current, labelSelector, targetKsvc, targetURL, reqCap)
			if err != nil {
				return 0, err
			}
			if sink != nil {
				sink(rt)
			}
			samples = append(samples, rt)
			sumMs += rt.Milliseconds()
			if over || rt.Milliseconds() > sloCold {
				f++
			}
			logging.Normal(fmt.Sprintf("[cmin-cold] cpu=%s sample %d/%d rt=%s over=%v (f=%d)", cpu, i+1, cMinColdSamples, rt, over, f))

			// Early-exit FAIL:
			//   percentile metric (allowedOver≥0): once more than allowedOver
			//     samples exceed the SLO, the nearest-rank gate must fail.
			//   avg metric (allowedOver<0): samples are non-negative, so the
			//     final mean ≥ sumMs/cMinColdSamples; if that lower bound already
			//     exceeds the SLO, the mean can never come back under it.
			failEarly := false
			if allowedOver >= 0 {
				failEarly = f > allowedOver
			} else {
				failEarly = sumMs > sloCold*int64(cMinColdSamples)
			}
			if failEarly {
				appendCMinColdSample(current, node, cpu, computeProbeStats(samples))
				memo[cpu] = sloCold + 1
				logging.Warning(fmt.Sprintf("[cmin-cold] cpu=%s FAILS SLO early after %d cold starts (f=%d sum=%dms)", cpu, i+1, f, sumMs))
				return sloCold + 1, nil
			}
		}
		stats := computeProbeStats(samples)
		appendCMinColdSample(current, node, cpu, stats)
		v := gate(stats).Milliseconds()
		memo[cpu] = v
		logging.Info(fmt.Sprintf("[cmin-cold] cpu=%s PASS gate=%dms ≤ slo=%dms (%d samples, f=%d)", cpu, v, sloCold, cMinColdSamples, f))
		return v, nil
	}

	// Lower bound: a probed failing point if we have one, else confirm the floor fails.
	var lo string
	if loSeed != "" {
		lo = loSeed
	} else {
		floorCpu := fmt.Sprintf("%dm", minProbeCpuMilli)
		floorMs, err := measure(floorCpu)
		if err != nil {
			return "", err
		}
		if floorMs <= sloCold {
			logging.Success(fmt.Sprintf("[cmin-cold] safety floor %s already meets SLO → c_min=%s", floorCpu, floorCpu))
			return floorCpu, nil
		}
		lo = floorCpu
	}
	logging.Info(fmt.Sprintf("[cmin-cold] downward search bracket: lo=%s hi=%s", lo, hi))

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
		if ms <= sloCold {
			hi = mid // meets — go lower
		} else {
			lo = mid // fails — go higher
		}
	}
	logging.Success(fmt.Sprintf("[cmin-cold] downward search complete: c_min=%s (slo=%dms)", hi, sloCold))
	return hi, nil
}
