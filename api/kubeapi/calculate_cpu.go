package kubeapi

import (
	"fmt"

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
