package kubeapi

import (
	"context"
	"encoding/json"
	"fmt"

	"nimbus/api/logging"
	"nimbus/api/nimbusevent"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// WriteNimbusStatus persists the per-node converged CPU values to the Nimbus
// CRD's .status.perNode map. Once every candidate node has both startingCpu
// and runningCpu populated, a subsequent watch Added event (controller
// restart, re-apply) takes the worker's fast path instead of re-probing.
//
// To force a re-search, clear the map:
//
//	kubectl patch nimbus <name> -n <ns> --subresource=status --type=merge \
//	    -p '{"status":{"perNode":null}}'
func WriteNimbusStatus(ctx context.Context, namespace, name string, perNode map[string]*nimbusevent.NodeResult) error {
	statusMap := make(map[string]map[string]string, len(perNode))
	for node, r := range perNode {
		if r == nil {
			continue
		}
		statusMap[node] = map[string]string{
			"startingCpu": r.StartingCpu,
			"runningCpu":  r.RunningCpu,
		}
	}

	payload := map[string]interface{}{
		"status": map[string]interface{}{
			"perNode": statusMap,
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
		"status",
	)
	if err != nil {
		logging.Failure("Failed to write Nimbus status:", err)
		return err
	}

	logging.Success(fmt.Sprintf("Nimbus status persisted: %s/%s perNode=%d entries",
		namespace, name, len(statusMap)))
	return nil
}
