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
	statusMap := make(map[string]interface{}, len(perNode))
	for node, r := range perNode {
		if r == nil {
			continue
		}
		entry := map[string]interface{}{
			"startingCpu": r.StartingCpu,
			"runningCpu":  r.RunningCpu,
		}
		// Only include sample arrays when populated — keeps partial-progress
		// patches small and lets the CRD's omitempty stay meaningful.
		if len(r.ColdRtSamples) > 0 {
			entry["coldRtSamples"] = r.ColdRtSamples
		}
		if len(r.WarmRtSamples) > 0 {
			entry["warmRtSamples"] = r.WarmRtSamples
		}
		statusMap[node] = entry
	}

	// Cluster-wide aggregates — max over per-node so the slowest node
	// still meets the configured RT budget. Surfaced on .status alongside
	// the per-node map so operators can see, via `kubectl get nimbus -o
	// yaml`, exactly which CPU values the controller is applying.
	statusPayload := map[string]interface{}{
		"perNode": statusMap,
	}
	if runningMax := MaxRunningCpu(perNode); runningMax != "" {
		statusPayload["runningCpu"] = runningMax
	}
	if startingMax := MaxStartingCpu(perNode); startingMax != "" {
		statusPayload["startingCpu"] = startingMax
	}
	payload := map[string]interface{}{
		"status": statusPayload,
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

// WriteAppliedStatus persists the per-ksvc apply outcome from one tick to
// .status.applied. Keys are ksvc names; values reflect what NIMBUS wrote
// (or attempted to write) onto the ksvc during the apply loop. The map
// is sent as a whole because merge-patch semantics on this subkey would
// otherwise leak stale entries from previous values[] sets.
//
// `nil` and empty maps both clear .status.applied. The caller is
// responsible for not writing partial state — pass the full set of ksvcs
// the apply loop iterated over.
//
// To force-clear:
//
//	kubectl patch nimbus <name> -n <ns> --subresource=status --type=merge \
//	    -p '{"status":{"applied":null}}'
func WriteAppliedStatus(ctx context.Context, namespace, name string, applied map[string]nimbusevent.KsvcApplyState) error {
	// Use explicit `null` to clear so a tick with no entries still gets
	// recorded as "nothing applied" rather than leaving stale data.
	var appliedValue interface{}
	if len(applied) == 0 {
		appliedValue = nil
	} else {
		appliedValue = applied
	}

	payload := map[string]interface{}{
		"status": map[string]interface{}{
			"applied": appliedValue,
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
		logging.Failure("Failed to write Nimbus apply status:", err)
		return err
	}

	logging.Success(fmt.Sprintf("Nimbus apply status persisted: %s/%s applied=%d entries",
		namespace, name, len(applied)))
	return nil
}
