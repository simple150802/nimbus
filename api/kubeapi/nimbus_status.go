package kubeapi

import (
	"context"
	"encoding/json"

	"nimbus/api/logging"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// WriteNimbusStatus persists the finalized startingCpu and runningCpu to the
// Nimbus CRD's .status subresource. Once written, a subsequent watch Added
// event (e.g., after controller restart or CRD re-apply) will see the
// populated status and skip re-running the binary search.
//
// To force a re-search, clear the fields:
//
//	kubectl patch nimbus <name> -n <ns> --subresource=status --type=merge \
//	    -p '{"status":{"startingCpu":"","runningCpu":""}}'
func WriteNimbusStatus(ctx context.Context, namespace, name, startingCPU, runningCPU string) error {
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

	_, err = DYNCLIENT.Resource(NIMBUS_GVR).Namespace(namespace).Patch(
		ctx,
		name,
		types.MergePatchType,
		payloadBytes,
		metav1.PatchOptions{},
		"status", // target the /status subresource
	)
	if err != nil {
		logging.Failure("Failed to write Nimbus status:", err)
		return err
	}

	logging.Success("Nimbus status persisted:", namespace+"/"+name,
		"starting=", startingCPU, "running=", runningCPU)
	return nil
}
