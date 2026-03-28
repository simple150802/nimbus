package kubeapi

import (
	"context"
	"lazyken-controller/api/boostevent"
	"lazyken-controller/api/logging"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	// Map your Struct data into the Unstructured format
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
								"requests": cpuValue,
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

	_, err := DYNCLIENT.Resource(STD_GVR).Namespace(event.Metadata.Namespace).Create(ctx, cr, metav1.CreateOptions{})
	if err != nil {
		logging.Failure("Failed to create CRD:", err)
	}

	logging.Success("Successfully created StartupCPUBoost!")
}
