package kubeapi

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
)

// minCPUFloorMilli is the value AdjustCPUMilli falls back to when an
// applied offset would drop the result to zero or below. Stops the binary
// search from probing at impossibly small CPU.
const minCPUFloorMilli int64 = 50

// CalculateAverageCPU returns the midpoint of [minStr, maxStr] as a
// Kubernetes-quantity string with milli-CPU precision (e.g. "550m").
func CalculateAverageCPU(minStr, maxStr string) (string, error) {
	minQty, err := resource.ParseQuantity(minStr)
	if err != nil {
		return "", err
	}
	maxQty, err := resource.ParseQuantity(maxStr)
	if err != nil {
		return "", err
	}
	avgMilli := (minQty.MilliValue() + maxQty.MilliValue()) / 2
	return fmt.Sprintf("%dm", avgMilli), nil
}

// AdjustCPUMilli adds offsetMilli to currentCPU and returns the result as
// a milli-CPU string. Accepts either "Nm" or whole-core ("0.5") inputs.
// Floors at minCPUFloorMilli to avoid probing zero or negative CPU.
func AdjustCPUMilli(currentCPU string, offsetMilli int64) (string, error) {
	var currentMilli int64
	if strings.HasSuffix(currentCPU, "m") {
		trimmed := strings.TrimSuffix(currentCPU, "m")
		v, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return "", fmt.Errorf("invalid mCPU format: %v", err)
		}
		currentMilli = v
	} else {
		val, err := strconv.ParseFloat(currentCPU, 64)
		if err != nil {
			return "", fmt.Errorf("invalid CPU core format: %v", err)
		}
		currentMilli = int64(val * 1000)
	}

	newMilli := currentMilli + offsetMilli
	if newMilli <= 0 {
		newMilli = minCPUFloorMilli
	}
	return fmt.Sprintf("%dm", newMilli), nil
}

// IsDiffGreaterThresh reports whether maxStr - minStr (in milli-CPU)
// exceeds thresholdMilli. The binary search uses this to decide when to
// stop narrowing the [low, high] window.
func IsDiffGreaterThresh(minStr, maxStr string, thresholdMilli int64) (bool, error) {
	minQty, err := resource.ParseQuantity(minStr)
	if err != nil {
		return false, err
	}
	maxQty, err := resource.ParseQuantity(maxStr)
	if err != nil {
		return false, err
	}
	return maxQty.MilliValue()-minQty.MilliValue() > thresholdMilli, nil
}
