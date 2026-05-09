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
)

const (
	// runningPhaseLowOffsetMilli is the milli-CPU offset applied to the
	// spec's min when seeding the running-phase search lower bound. The
	// running-phase optimum is allowed to dip below the starting-phase
	// minimum, so we extend the range slightly downward.
	runningPhaseLowOffsetMilli = -50

	// convergenceThresholdMilli stops the binary search when high - low
	// drops below this threshold (milli-CPU). 100m corresponds to roughly
	// the granularity Kubernetes' scheduler can act on.
	convergenceThresholdMilli = 100

	// responseTimeImprovementGate is the relative drop in measured
	// response time needed to justify moving the lower bound up rather
	// than narrowing toward the upper bound. 0.10 == 10%.
	responseTimeImprovementGate = 0.10
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
	logging.Info(fmt.Sprintf("[node=%s] starting CPU: %s | running CPU: %s",
		node, result.StartingCpu, result.RunningCpu))

	return current.High, nil
}

// probeFn is a phase-agnostic measurement primitive. The starting phase
// passes a closure around getResptCold; the running phase passes one
// around getResptWarm. Both return a duration to compare against the
// previous probe.
type probeFn func(ctx context.Context, current *nimbusevent.NimbusEvent, cpu string) (time.Duration, error)

// runBinarySearch is the shared convergence loop used by both phases.
// It walks the [low, high] window, asking probe() for the response time
// at low, mid, and high, and chooses which bound to move based on the
// 10% improvement gate. Stops when high - low <= convergenceThresholdMilli.
// On success it writes setResult(current.High) and returns current.High.
//
// recordSample is invoked once per probe call with the (cpu, rt) pair
// the probe just measured. The per-phase wrapper appends these into the
// active node's ColdRtSamples / WarmRtSamples slice; runBinarySearch
// itself stays storage-agnostic.
func runBinarySearch(
	ctx context.Context,
	current *nimbusevent.NimbusEvent,
	probe probeFn,
	setResult func(cpu string),
	recordSample func(cpu string, rt time.Duration),
) (string, error) {
	rtLow, err := probe(ctx, current, current.Low)
	if err != nil {
		return "", err
	}
	recordSample(current.Low, rtLow)

	for {
		shouldContinue, err := kubeapi.IsDiffGreaterThresh(current.Low, current.High, convergenceThresholdMilli)
		if err != nil {
			logging.Failure("Error calculating threshold:", err)
			return "", err
		}
		if !shouldContinue {
			logging.Success(fmt.Sprintf("Binary Search Complete! The optimal CPU limit is: %s", current.High))
			setResult(current.High)
			return current.High, nil
		}

		midCPU, err := kubeapi.CalculateAverageCPU(current.Low, current.High)
		if err != nil {
			logging.Failure("Invalid CPU units:", err)
			return "", err
		}
		logging.Info("Checking at", midCPU, "CPU ...")

		rtMid, err := probe(ctx, current, midCPU)
		if err != nil {
			return "", err
		}
		recordSample(midCPU, rtMid)

		if float64(rtLow-rtMid)/float64(rtLow) > responseTimeImprovementGate {
			rtHigh, err := probe(ctx, current, current.High)
			if err != nil {
				return "", err
			}
			recordSample(current.High, rtHigh)
			if float64(rtMid-rtHigh)/float64(rtMid) > responseTimeImprovementGate {
				current.Low = midCPU
				rtLow = rtMid
			} else {
				current.High = midCPU
			}
		} else {
			current.High = midCPU
		}
	}
}

// sortSamplesByCpu canonicalizes a sample list so the persisted order
// matches what the online stage's piecewise-linear interpolator expects
// (ascending cpu). Called once at end-of-phase, after all samples for
// a (node, phase) tuple have been collected.
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

func binarySearchForStartingPhase(ctx context.Context, current *nimbusevent.NimbusEvent, node string) (string, error) {
	current.Low = current.Spec.ResourcePolicy.ContainerPolicies[0].ResourceRange.Limits.Min
	current.High = current.Spec.ResourcePolicy.ContainerPolicies[0].ResourceRange.Limits.Max

	probe := func(ctx context.Context, ev *nimbusevent.NimbusEvent, cpu string) (time.Duration, error) {
		return getResptCold(ctx, ev, cpu)
	}
	setResult := func(cpu string) {
		current.PerNodeResults[node].StartingCpu = cpu
		current.PerNodeResults[node].StartingSaturated = true
	}
	recordSample := func(cpu string, rt time.Duration) {
		current.PerNodeResults[node].ColdRtSamples = append(
			current.PerNodeResults[node].ColdRtSamples,
			nimbusevent.SamplePoint{Cpu: cpu, RtMillis: rt.Milliseconds()},
		)
	}
	cpu, err := runBinarySearch(ctx, current, probe, setResult, recordSample)
	sortSamplesByCpu(current.PerNodeResults[node].ColdRtSamples)
	return cpu, err
}

func binarySearchForRunningPhase(ctx context.Context, current *nimbusevent.NimbusEvent, node string) (string, error) {
	current.Low = current.Spec.ResourcePolicy.ContainerPolicies[0].ResourceRange.Limits.Min
	runningLow, err := kubeapi.AdjustCPUMilli(current.Low, runningPhaseLowOffsetMilli)
	if err != nil {
		return "", err
	}
	current.Low = runningLow
	current.High = current.PerNodeResults[node].StartingCpu

	probe := func(ctx context.Context, ev *nimbusevent.NimbusEvent, cpu string) (time.Duration, error) {
		return getResptWarm(ctx, ev, cpu, current.High)
	}
	setResult := func(cpu string) {
		current.PerNodeResults[node].RunningCpu = cpu
		current.PerNodeResults[node].RunningSaturated = true
	}
	recordSample := func(cpu string, rt time.Duration) {
		current.PerNodeResults[node].WarmRtSamples = append(
			current.PerNodeResults[node].WarmRtSamples,
			nimbusevent.SamplePoint{Cpu: cpu, RtMillis: rt.Milliseconds()},
		)
	}
	cpu, err := runBinarySearch(ctx, current, probe, setResult, recordSample)
	sortSamplesByCpu(current.PerNodeResults[node].WarmRtSamples)
	return cpu, err
}
