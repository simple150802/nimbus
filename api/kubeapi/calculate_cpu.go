package kubeapi

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
)

func CalculateAverageCPU(minStr, maxStr string) (string, error) {
	// 1. Parse the strings into Quantity objects
	minQty, err := resource.ParseQuantity(minStr)
	if err != nil {
		return "", err
	}
	maxQty, err := resource.ParseQuantity(maxStr)
	if err != nil {
		return "", err
	}

	// 2. Convert to MilliValue (int64)
	// This turns "1" into 1000 and "100m" into 100
	minMilli := minQty.MilliValue()
	maxMilli := maxQty.MilliValue()

	// 3. Calculate the Average (The Binary Search Midpoint)
	avgMilli := (minMilli + maxMilli) / 2

	// 4. Convert back to a Kubernetes string (e.g., "550m")
	// We use "m" suffix to ensure precision
	return fmt.Sprintf("%dm", avgMilli), nil
}

// AdjustCPUMilli takes a current CPU string (like "100m" or "0.5")
// and adds/subtracts the offset in milli-CPU.
func AdjustCPUMilli(currentCPU string, offsetMilli int64) (string, error) {
	var currentMilli int64
	var err error

	// 1. Parse the current value into milli-CPU
	if strings.HasSuffix(currentCPU, "m") {
		// Case: "100m"
		trimmed := strings.TrimSuffix(currentCPU, "m")
		currentMilli, err = strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return "", fmt.Errorf("invalid mCPU format: %v", err)
		}
	} else {
		// Case: "0.5" (Cores)
		val, err := strconv.ParseFloat(currentCPU, 64)
		if err != nil {
			return "", fmt.Errorf("invalid CPU core format: %v", err)
		}
		currentMilli = int64(val * 1000)
	}

	// 2. Apply the offset
	newMilli := currentMilli + offsetMilli

	// 3. Updated Safety Guardrail:
	// If the result is 0 or negative (below zero), set to 10m
	if newMilli <= 0 {
		newMilli = 50
	}

	return fmt.Sprintf("%dm", newMilli), nil
}

func IsDiffGreaterThresh(minStr, maxStr string, thresholdMilli int64) (bool, error) {
	// 1. Parse the strings
	minQty, err := resource.ParseQuantity(minStr)
	if err != nil {
		return false, err
	}
	maxQty, err := resource.ParseQuantity(maxStr)
	if err != nil {
		return false, err
	}

	// 2. Convert to MilliValue
	minMilli := minQty.MilliValue()
	maxMilli := maxQty.MilliValue()

	// 3. Compare the difference
	// We use max - min to see if the search range is still wide
	diff := maxMilli - minMilli

	// Returns true if the difference is more than 100 (milli)
	return diff > thresholdMilli, nil
}
