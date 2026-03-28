package algorithm

import (
	"context"
	"fmt"
	"lazyken-controller/api/boostevent"
	"lazyken-controller/api/kubeapi"
	"lazyken-controller/api/logging"
)

func BinarySearch(ctx context.Context, current *boostevent.BoostEvent) (string, error) {
	current.Low = current.Spec.ResourcePolicy.ContainerPolicies[0].ResourceRange.Limits.Min
	current.High = current.Spec.ResourcePolicy.ContainerPolicies[0].ResourceRange.Limits.Max

	rtLow, err := getRespt(ctx, current, current.Low)
	if err != nil {
		logging.Failure("Skipping this CPU value due to error:", err)
		// Note: Depending on your business logic, you might want to return here
		// if rtLow is invalid, to avoid a divide-by-zero panic later.
	}

	for {
		shouldContinue, err := kubeapi.IsDiffGreaterThresh(current.Low, current.High, 100)
		if err != nil {
			logging.Failure("Error calculating threshold:", err)
			return "", err
		}

		if !shouldContinue {
			logging.Success(fmt.Sprintf("Binary Search Complete! The optimal CPU limit is: %s", current.High))
			return current.High, nil // Return the optimal value
		}

		midCPU, err := kubeapi.CalculateAverageCPU(current.Low, current.High)
		if err != nil {
			logging.Failure("Invalid CPU units:", err)
			return "", err
		}

		logging.Info("Checking at", midCPU, "CPU ...")

		rtMid, err := getRespt(ctx, current, midCPU)
		if err != nil {
			logging.Failure("Skipping this CPU value due to error:", err)
		}

		// If response time improved by more than 10%, move the lower bound up
		if float64(rtLow-rtMid)/float64(rtLow) > 0.1 {
			// current.Low = midCPU
			// rtLow = rtMid
			// Change
			rtHigh, err := getRespt(ctx, current, current.High)
			if err != nil {
				logging.Failure("Invalid CPU units:", err)
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
