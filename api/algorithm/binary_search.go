package algorithm

import (
	"context"
	"fmt"
	"time"

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
func runBinarySearch(
	ctx context.Context,
	current *nimbusevent.NimbusEvent,
	probe probeFn,
	setResult func(cpu string),
) (string, error) {
	rtLow, err := probe(ctx, current, current.Low)
	if err != nil {
		return "", err
	}

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

		if float64(rtLow-rtMid)/float64(rtLow) > responseTimeImprovementGate {
			rtHigh, err := probe(ctx, current, current.High)
			if err != nil {
				return "", err
			}
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
	return runBinarySearch(ctx, current, probe, setResult)
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
	return runBinarySearch(ctx, current, probe, setResult)
}
