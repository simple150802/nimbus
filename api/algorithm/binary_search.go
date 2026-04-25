package algorithm

import (
	"fmt"

	"context"
	"recon/api/boostevent"
	"recon/api/kubeapi"
	"recon/api/logging"
)

func BinarySearch(ctx context.Context, current *boostevent.BoostEvent) (string, error) {
	ns := current.Metadata.Namespace
	ksvc := current.Selector.MatchExpressions[0].Values[0]

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

	if _, err := binarySearchForStartingPhase(ctx, current); err != nil {
		return "", fmt.Errorf("starting phase aborted: %w", err)
	}
	if _, err := binarySearchForRunningPhase(ctx, current); err != nil {
		return "", fmt.Errorf("running phase aborted: %w", err)
	}

	logging.Info("CPU for starting phase: ", current.StartingCPU)
	logging.Info("CPU for running phase: ", current.RunningCPU)

	return current.High, nil
}

func binarySearchForRunningPhase(ctx context.Context, current *boostevent.BoostEvent) (string, error) {
	// NOTE: Resource_limit of pod during starting phase must be higher than in running phase
	// If not, an err will occur (Fix in future), currently just pray for err not occur ^^
	current.Low = current.Spec.ResourcePolicy.ContainerPolicies[0].ResourceRange.Limits.Min
	runningLow, err := kubeapi.AdjustCPUMilli(current.Low, -50)
	if err != nil {
		return "", err
	}
	current.Low = runningLow
	current.High = current.StartingCPU

	rtLow, err := getResptWarm(ctx, current, current.Low, current.High)
	if err != nil {
		return "", err
	}

	for {
		shouldContinue, err := kubeapi.IsDiffGreaterThresh(current.Low, current.High, 100)
		if err != nil {
			logging.Failure("Error calculating threshold:", err)
			return "", err
		}

		if !shouldContinue {
			logging.Success(fmt.Sprintf("Binary Search Complete! The optimal CPU limit is: %s", current.High))
			current.RunningSaturated = true
			current.RunningCPU = current.High
			return current.RunningCPU, nil
		}

		midCPU, err := kubeapi.CalculateAverageCPU(current.Low, current.High)
		if err != nil {
			logging.Failure("Invalid CPU units:", err)
			return "", err
		}

		logging.Info("Checking at", midCPU, "CPU ...")

		rtMid, err := getResptWarm(ctx, current, midCPU, current.High)
		if err != nil {
			return "", err
		}

		// If response time improved by more than 10%, move the lower bound up
		if float64(rtLow-rtMid)/float64(rtLow) > 0.1 {
			rtHigh, err := getResptWarm(ctx, current, current.High, current.High)
			if err != nil {
				return "", err
			}
			if float64(rtMid-rtHigh)/float64(rtMid) > 0.1 {
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

func binarySearchForStartingPhase(ctx context.Context, current *boostevent.BoostEvent) (string, error) {
	current.Low = current.Spec.ResourcePolicy.ContainerPolicies[0].ResourceRange.Limits.Min
	current.High = current.Spec.ResourcePolicy.ContainerPolicies[0].ResourceRange.Limits.Max

	rtLow, err := getResptCold(ctx, current, current.Low)
	if err != nil {
		return "", err
	}

	for {
		shouldContinue, err := kubeapi.IsDiffGreaterThresh(current.Low, current.High, 100)
		if err != nil {
			logging.Failure("Error calculating threshold:", err)
			return "", err
		}

		if !shouldContinue {
			logging.Success(fmt.Sprintf("Binary Search Complete! The optimal CPU limit is: %s", current.High))
			current.StartingSaturated = true
			current.StartingCPU = current.High
			return current.StartingCPU, nil
		}

		midCPU, err := kubeapi.CalculateAverageCPU(current.Low, current.High)
		if err != nil {
			logging.Failure("Invalid CPU units:", err)
			return "", err
		}

		logging.Info("Checking at", midCPU, "CPU ...")

		rtMid, err := getResptCold(ctx, current, midCPU)
		if err != nil {
			return "", err
		}

		// If response time improved by more than 10%, move the lower bound up
		if float64(rtLow-rtMid)/float64(rtLow) > 0.1 {
			rtHigh, err := getResptCold(ctx, current, current.High)
			if err != nil {
				return "", err
			}
			if float64(rtMid-rtHigh)/float64(rtMid) > 0.1 {
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
