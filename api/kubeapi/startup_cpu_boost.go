package kubeapi

import (
	"context"
	"encoding/json"
	"fmt"

	"nimbus/api/logging"
	"nimbus/api/nimbusevent"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// DeleteStartupCPUBoost removes the StartupCPUBoost CR named name from
// namespace. Errors are logged, not returned — callers treat cleanup as
// best-effort.
func DeleteStartupCPUBoost(ctx context.Context, namespace string, name string) {
	logging.Info("Cleaning up old StartupCPUBoost CR:", name)
	if err := DYNCLIENT.Resource(STD_GVR).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		logging.Failure("failed to delete CRD:", err)
	}
	logging.Success("Successfully cleaned up cluster state!")
}

// CreateStartupCPUBoost upserts ONE StartupCPUBoost CR per ksvc. The CR
// is named "<nimbus-name>-<ksvcName>" with selector scoped to just this
// ksvc, labels identifying the owning Nimbus, and an apiCondition.url
// auto-built from the ksvc + namespace + spec.durationPolicy.coldApiCondition.path.
// (The upstream boost CRD's field is still spelled "apiCondition"; we
// only renamed the field on our side.)
// Callers loop over selector.matchExpressions[0].values to fan out one
// boost CR per ksvc; the upstream kube-startup-cpu-boost controller then
// runs an independent boost lifecycle per ksvc.
// Returns changed=true only when it actually created or patched the CR; a
// converged no-op (CR already matches) returns false so the online
// reconciler can stay silent when nothing moved.
func CreateStartupCPUBoost(ctx context.Context, event *nimbusevent.NimbusEvent, ksvcName, cpuValue string) (changed bool) {
	if len(event.Selector.MatchExpressions) == 0 || len(event.Spec.ResourcePolicy.ContainerPolicies) == 0 {
		logging.Failure("CreateStartupCPUBoost: Nimbus is missing selector or container policy — skipping")
		return false
	}

	crName := event.Metadata.Name + "-" + ksvcName
	cr := buildBoostCR(event, ksvcName, crName, cpuValue)
	resourceClient := DYNCLIENT.Resource(STD_GVR).Namespace(event.Metadata.Namespace)

	existing, err := resourceClient.Get(ctx, crName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			logging.Info(fmt.Sprintf("[set] StartupCPUBoost -> ns=%s name=%s ksvc=%s limits=%s",
				event.Metadata.Namespace, crName, ksvcName, cpuValue))
			if _, err := resourceClient.Create(ctx, cr, metav1.CreateOptions{}); err != nil {
				logging.Failure("Failed to create CRD:", err)
				return false
			}
			logging.Success("Successfully created StartupCPUBoost!")
			return true
		}
		logging.Failure("Error checking for existing CRD:", err)
		return false
	}

	// No-op when the existing boost CR already carries the desired CPU,
	// poll URL, and gate body — a converged reconcile tick issues no write
	// and logs nothing.
	if boostCRConverged(existing, event, ksvcName, cpuValue) {
		return false
	}

	// MergePatch updates fields without replacing the whole object.
	logging.Info(fmt.Sprintf("[set] StartupCPUBoost -> ns=%s name=%s ksvc=%s limits=%s",
		event.Metadata.Namespace, crName, ksvcName, cpuValue))
	payloadBytes, _ := json.Marshal(cr.Object)
	if _, err := resourceClient.Patch(ctx, crName, types.MergePatchType, payloadBytes, metav1.PatchOptions{}); err != nil {
		logging.Failure("Failed to patch existing CRD:", err)
		return false
	}
	logging.Success("Successfully updated existing StartupCPUBoost!")
	return true
}

// boostCRConverged reports whether an existing StartupCPUBoost CR already
// carries everything CreateStartupCPUBoost would write for this ksvc: the
// fixed CPU (requests==limits==cpuValue, compared as quantities so "1000m"
// matches "1"), the per-ksvc apiCondition.url, and the cold-gate response
// body. When true the caller skips the MergePatch entirely. Any missing or
// mismatched field returns false so the patch re-asserts desired state.
func boostCRConverged(existing *unstructured.Unstructured, event *nimbusevent.NimbusEvent, ksvcName, cpuValue string) bool {
	cond := event.Spec.DurationPolicy.ColdApiCondition
	wantURL := BuildKsvcStatusURL(event.Metadata.Namespace, ksvcName, cond.Path)

	cps, found, err := unstructured.NestedSlice(existing.Object, "spec", "resourcePolicy", "containerPolicies")
	if err != nil || !found || len(cps) == 0 {
		return false
	}
	cp0, ok := cps[0].(map[string]interface{})
	if !ok {
		return false
	}
	curReq, _, _ := unstructured.NestedString(cp0, "fixedResources", "requests")
	curLim, _, _ := unstructured.NestedString(cp0, "fixedResources", "limits")
	if !cpuEqual(curReq, cpuValue) || !cpuEqual(curLim, cpuValue) {
		return false
	}

	curURL, _, _ := unstructured.NestedString(existing.Object, "spec", "durationPolicy", "apiCondition", "url")
	if curURL != wantURL {
		return false
	}
	curResp, _, _ := unstructured.NestedString(existing.Object, "spec", "durationPolicy", "apiCondition", "response")
	return curResp == cond.Response
}

// buildBoostCR constructs the StartupCPUBoost CR object for one ksvc.
// The selector targets a single ksvc; the apiCondition.url is derived
// per ksvc via BuildKsvcStatusURL so each boost CR's poll target matches
// its own ksvc instance. The boost CR always uses the COLD condition —
// the upstream webhook polls for pod-readiness, which is cold-phase by
// definition.
func buildBoostCR(event *nimbusevent.NimbusEvent, ksvcName, crName, cpuValue string) *unstructured.Unstructured {
	expr := event.Selector.MatchExpressions[0]
	policy := event.Spec.ResourcePolicy.ContainerPolicies[0]
	cond := event.Spec.DurationPolicy.ColdApiCondition
	ksvcURL := BuildKsvcStatusURL(event.Metadata.Namespace, ksvcName, cond.Path)

	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "autoscaling.x-k8s.io/v1alpha1",
			"kind":       "StartupCPUBoost",
			"metadata": map[string]interface{}{
				"name":      crName,
				"namespace": event.Metadata.Namespace,
				"labels": map[string]interface{}{
					"app.kubernetes.io/managed-by": "nimbus",
					"nimbus.io/owned-by":           event.Metadata.Name,
					"nimbus.io/ksvc":               ksvcName,
				},
			},
			"selector": map[string]interface{}{
				"matchExpressions": []map[string]interface{}{
					{
						"key":      expr.Key,
						"operator": expr.Operator,
						"values":   []string{ksvcName},
					},
				},
			},
			"spec": map[string]interface{}{
				"resourcePolicy": map[string]interface{}{
					"containerPolicies": []map[string]interface{}{
						{
							"containerName": policy.ContainerName,
							"fixedResources": map[string]interface{}{
								// requests == limits → Guaranteed QoS during
								// the boost window. Predictable bin-packing
								// (scheduler reserves cpuValue, not a floor)
								// and the pod is never throttled below the
								// probed CPU under contention.
								"requests": cpuValue,
								"limits":   cpuValue,
							},
						},
					},
				},
				"durationPolicy": map[string]interface{}{
					"apiCondition": map[string]interface{}{
						"url":      ksvcURL,
						"response": cond.Response,
					},
				},
			},
		},
	}
}
