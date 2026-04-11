package kubeapi

import (
	"context"
	"encoding/json"
	"lazyken-controller/api/boostevent"
	"lazyken-controller/api/logging"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func DeleteStartupCPUBoost(ctx context.Context, namespace string, name string) {
	logging.Info("Cleaning up old StartupCPUBoost CR:", name)

	err := DYNCLIENT.Resource(STD_GVR).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil {
		logging.Failure("failed to delete CRD:", err)
	}

	logging.Success("Successfully cleaned up cluster state!")
}

func CreateStartupCPUBoost(ctx context.Context, event *boostevent.BoostEvent, cpuValue string) {
	// 1. Define the desired state (same as your current map)
	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "autoscaling.x-k8s.io/v1alpha1",
			"kind":       "StartupCPUBoost",
			"metadata": map[string]interface{}{
				"name":      event.Metadata.Name,
				"namespace": event.Metadata.Namespace,
			},
			"selector": map[string]interface{}{
				"matchExpressions": []map[string]interface{}{
					{
						"key":      event.Selector.MatchExpressions[0].Key,
						"operator": event.Selector.MatchExpressions[0].Operator,
						"values":   event.Selector.MatchExpressions[0].Values,
					},
				},
			},
			"spec": map[string]interface{}{
				"resourcePolicy": map[string]interface{}{
					"containerPolicies": []map[string]interface{}{
						{
							"containerName": event.Spec.ResourcePolicy.ContainerPolicies[0].ContainerName,
							"fixedResources": map[string]interface{}{
								"requests": "150m",
								"limits":   cpuValue,
							},
						},
					},
				},
				"durationPolicy": map[string]interface{}{
					"apiCondition": map[string]interface{}{
						"url":      event.Spec.DurationPolicy.ApiCondition.Url,
						"response": event.Spec.DurationPolicy.ApiCondition.Response,
					},
				},
			},
		},
	}

	resourceClient := DYNCLIENT.Resource(STD_GVR).Namespace(event.Metadata.Namespace)

	// 2. Check if it already exists
	_, err := resourceClient.Get(ctx, event.Metadata.Name, metav1.GetOptions{})

	if err != nil {
		if errors.IsNotFound(err) {
			// 3. CASE: Doesn't exist -> CREATE
			_, err = resourceClient.Create(ctx, cr, metav1.CreateOptions{})
			if err != nil {
				logging.Failure("Failed to create CRD:", err)
				return
			}
			logging.Success("Successfully created StartupCPUBoost!")
		} else {
			// CASE: System error
			logging.Failure("Error checking for existing CRD:", err)
		}
		return
	}

	// 4. CASE: Exists -> PATCH
	payloadBytes, _ := json.Marshal(cr.Object)
	_, err = resourceClient.Patch(
		ctx,
		event.Metadata.Name,
		types.MergePatchType, // MergePatch updates fields without replacing the whole object
		payloadBytes,
		metav1.PatchOptions{},
	)

	if err != nil {
		logging.Failure("Failed to patch existing CRD:", err)
		return
	}

	logging.Success("Successfully updated existing StartupCPUBoost!")
}
