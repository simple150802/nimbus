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

// defaultBoostRequestCPU is the CPU `requests` value baked into every
// StartupCPUBoost CR we create. limits varies per probe; requests stays
// fixed because the upstream webhook doesn't surface it through the
// Nimbus CRD.
const defaultBoostRequestCPU = "150m"

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
// auto-built from the ksvc + namespace + spec.durationPolicy.apiCondition.path.
// Callers loop over selector.matchExpressions[0].values to fan out one
// boost CR per ksvc; the upstream kube-startup-cpu-boost controller then
// runs an independent boost lifecycle per ksvc.
func CreateStartupCPUBoost(ctx context.Context, event *nimbusevent.NimbusEvent, ksvcName, cpuValue string) {
	if len(event.Selector.MatchExpressions) == 0 || len(event.Spec.ResourcePolicy.ContainerPolicies) == 0 {
		logging.Failure("CreateStartupCPUBoost: Nimbus is missing selector or container policy — skipping")
		return
	}

	crName := event.Metadata.Name + "-" + ksvcName
	logging.Info(fmt.Sprintf("[set] StartupCPUBoost -> ns=%s name=%s ksvc=%s limits=%s",
		event.Metadata.Namespace, crName, ksvcName, cpuValue))

	cr := buildBoostCR(event, ksvcName, crName, cpuValue)
	resourceClient := DYNCLIENT.Resource(STD_GVR).Namespace(event.Metadata.Namespace)

	if _, err := resourceClient.Get(ctx, crName, metav1.GetOptions{}); err != nil {
		if errors.IsNotFound(err) {
			if _, err := resourceClient.Create(ctx, cr, metav1.CreateOptions{}); err != nil {
				logging.Failure("Failed to create CRD:", err)
				return
			}
			logging.Success("Successfully created StartupCPUBoost!")
			return
		}
		logging.Failure("Error checking for existing CRD:", err)
		return
	}

	// MergePatch updates fields without replacing the whole object.
	payloadBytes, _ := json.Marshal(cr.Object)
	if _, err := resourceClient.Patch(ctx, crName, types.MergePatchType, payloadBytes, metav1.PatchOptions{}); err != nil {
		logging.Failure("Failed to patch existing CRD:", err)
		return
	}
	logging.Success("Successfully updated existing StartupCPUBoost!")
}

// buildBoostCR constructs the StartupCPUBoost CR object for one ksvc.
// The selector targets a single ksvc; the apiCondition.url is derived
// per ksvc via BuildKsvcStatusURL so each boost CR's poll target matches
// its own ksvc instance.
func buildBoostCR(event *nimbusevent.NimbusEvent, ksvcName, crName, cpuValue string) *unstructured.Unstructured {
	expr := event.Selector.MatchExpressions[0]
	policy := event.Spec.ResourcePolicy.ContainerPolicies[0]
	cond := event.Spec.DurationPolicy.ApiCondition
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
								"requests": defaultBoostRequestCPU,
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
