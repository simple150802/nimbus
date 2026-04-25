package kubeapi

import (
	"context"
	"encoding/json"

	"recon/api/logging"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// WriteReconStatus persists the finalized startingCpu and runningCpu to the
// Recon CRD's .status subresource. Once written, a subsequent watch Added
// event (e.g., after controller restart or CRD re-apply) will see the
// populated status and skip re-running the binary search.
//
// To force a re-search, clear the fields:
//
//	kubectl patch recon <name> -n <ns> --subresource=status --type=merge \
//	    -p '{"status":{"startingCpu":"","runningCpu":""}}'
func WriteReconStatus(ctx context.Context, namespace, name, startingCPU, runningCPU string) error {
	payload := map[string]interface{}{
		"status": map[string]interface{}{
			"startingCpu": startingCPU,
			"runningCpu":  runningCPU,
		},
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	_, err = DYNCLIENT.Resource(RECON_GVR).Namespace(namespace).Patch(
		ctx,
		name,
		types.MergePatchType,
		payloadBytes,
		metav1.PatchOptions{},
		"status", // target the /status subresource
	)
	if err != nil {
		logging.Failure("Failed to write Recon status:", err)
		return err
	}

	logging.Success("Recon status persisted:", namespace+"/"+name,
		"starting=", startingCPU, "running=", runningCPU)
	return nil
}
