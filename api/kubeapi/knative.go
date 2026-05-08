package kubeapi

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"nimbus/api/logging"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// MonitorKsvcResources polls the ksvc's pods every 2 seconds and reports the
// user-container's CPU limit. The phase tag ("COLD" / "WARM") appears in
// every line so a scrolling log makes it obvious which probe is live.
func MonitorKsvcResources(ctx context.Context, phase, namespace, ksvcName string) {
	labelSelector := fmt.Sprintf("serving.knative.dev/service=%s", ksvcName)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	tag := phase
	if tag == "" {
		tag = "MON"
	}

	logging.Info(fmt.Sprintf("[%s][monitor] starting — ns=%s ksvc=%s", tag, namespace, ksvcName))

	for {
		select {
		case <-ctx.Done():
			logging.Info(fmt.Sprintf("[%s][monitor] stop signal received, shutting down", tag))
			return

		case <-ticker.C:
			pods, err := CLIENTSET.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
				LabelSelector: labelSelector,
			})
			if err != nil {
				logging.Failure(fmt.Sprintf("[%s][monitor] failed to fetch pods: %v", tag, err))
				continue
			}

			if len(pods.Items) == 0 {
				logging.Normal(fmt.Sprintf("[%s][monitor] ksvc scaled to 0 (no pods)", tag))
				continue
			}

			for _, pod := range pods.Items {
				for _, container := range pod.Spec.Containers {
					// Only report the user-container; queue-proxy has its own
					// Knative-managed resources and isn't relevant to the search.
					if container.Name != "user-container" {
						continue
					}
					cpuLim := container.Resources.Limits.Cpu().String()
					logging.Normal(fmt.Sprintf("[%s][monitor] pod=%s container=%s cpuLimit=%s",
						tag, pod.Name, container.Name, cpuLim))
				}
			}
		}
	}
}

// PatchResourceLimits updates the CPU limit on a Knative Service's
// container[0] via a JSON-patch replace. Logs the set event so the user can
// trace when a new CPU limit actually hits the ksvc.
func PatchResourceLimits(ctx context.Context, namespace, ksvcName, cpuLimit string) error {
	logging.Info(fmt.Sprintf("[set] ksvc cpu limit -> ns=%s ksvc=%s cpu=%s", namespace, ksvcName, cpuLimit))

	patchPayload := []map[string]interface{}{
		{
			"op":    "replace",
			"path":  "/spec/template/spec/containers/0/resources/limits/cpu",
			"value": cpuLimit,
		},
	}

	payloadBytes, err := json.Marshal(patchPayload)
	if err != nil {
		return err
	}

	_, err = DYNCLIENT.Resource(KSVC_GVR).Namespace(namespace).Patch(
		ctx,
		ksvcName,
		types.JSONPatchType,
		payloadBytes,
		metav1.PatchOptions{},
	)
	return err
}

// PatchMaxScale stamps autoscaling.knative.dev/max-scale=1 on the ksvc so
// the search probes against a single, deterministic pod.
func PatchMaxScale(ctx context.Context, namespace, ksvcName string) error {
	logging.Info(fmt.Sprintf("[set] ksvc maxScale=1 -> ns=%s ksvc=%s", namespace, ksvcName))

	patchPayload := []map[string]interface{}{
		{
			"op":    "add",
			"path":  "/spec/template/metadata/annotations/autoscaling.knative.dev~1max-scale",
			"value": "1",
		},
	}

	payloadBytes, err := json.Marshal(patchPayload)
	if err != nil {
		return err
	}

	_, err = DYNCLIENT.Resource(KSVC_GVR).Namespace(namespace).Patch(
		ctx,
		ksvcName,
		types.JSONPatchType,
		payloadBytes,
		metav1.PatchOptions{},
	)
	return err
}

// UnsetMaxScale removes the autoscaling.knative.dev/max-scale annotation via
// a JSON merge-patch with null. Paired with PatchMaxScale so the
// starting-phase cap doesn't carry into the running phase.
func UnsetMaxScale(ctx context.Context, namespace, ksvcName string) error {
	logging.Info(fmt.Sprintf("[set] ksvc maxScale=<unset> -> ns=%s ksvc=%s", namespace, ksvcName))

	payload := map[string]interface{}{
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{
					"annotations": map[string]interface{}{
						"autoscaling.knative.dev/max-scale": nil,
					},
				},
			},
		},
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = DYNCLIENT.Resource(KSVC_GVR).Namespace(namespace).Patch(
		ctx,
		ksvcName,
		types.MergePatchType,
		payloadBytes,
		metav1.PatchOptions{},
	)
	return err
}

// PinKsvcToNode constrains the ksvc to one node by adding a
// kubernetes.io/hostname key to spec.template.spec.nodeSelector via a
// JSON merge-patch. It composes (AND) with whatever nodeSelector or
// affinity the user already set — fine because the candidate node was
// computed from those constraints, so it already satisfies them. Paired
// with UnpinKsvc; each new pin overwrites the previous, so the per-node
// loop only needs one final unpin.
func PinKsvcToNode(ctx context.Context, namespace, ksvcName, node string) error {
	logging.Info(fmt.Sprintf("[set] ksvc pin -> ns=%s ksvc=%s node=%s", namespace, ksvcName, node))

	payload := map[string]interface{}{
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"spec": map[string]interface{}{
					"nodeSelector": map[string]interface{}{
						"kubernetes.io/hostname": node,
					},
				},
			},
		},
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = DYNCLIENT.Resource(KSVC_GVR).Namespace(namespace).Patch(
		ctx,
		ksvcName,
		types.MergePatchType,
		payloadBytes,
		metav1.PatchOptions{},
	)
	return err
}

// UnpinKsvc removes only the kubernetes.io/hostname key that PinKsvcToNode
// added. JSON merge-patch semantics with a null value drop just that key,
// leaving any user-set nodeSelector entries intact.
func UnpinKsvc(ctx context.Context, namespace, ksvcName string) error {
	logging.Info(fmt.Sprintf("[set] ksvc unpin -> ns=%s ksvc=%s", namespace, ksvcName))

	payload := map[string]interface{}{
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"spec": map[string]interface{}{
					"nodeSelector": map[string]interface{}{
						"kubernetes.io/hostname": nil,
					},
				},
			},
		},
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = DYNCLIENT.Resource(KSVC_GVR).Namespace(namespace).Patch(
		ctx,
		ksvcName,
		types.MergePatchType,
		payloadBytes,
		metav1.PatchOptions{},
	)
	return err
}

// DeleteKsvcPods force-deletes every pod matching the selector in namespace
// and returns the names of pods that were actually deleted. Used between
// cold probes so the next sample can't be served by a lingering pod whose
// CPU limit was injected under a prior StartupCPUBoost — the upstream boost
// webhook only fires at pod creation, so orphaned pods keep their old
// limits forever until they're scaled down or deleted. The caller logs the
// deletions with phase + sample context.
func DeleteKsvcPods(ctx context.Context, namespace, labelSelector string) ([]string, error) {
	pods, err := CLIENTSET.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, nil
	}
	zero := int64(0)
	var deleted []string
	for _, p := range pods.Items {
		err := CLIENTSET.CoreV1().Pods(namespace).Delete(ctx, p.Name, metav1.DeleteOptions{
			GracePeriodSeconds: &zero,
		})
		if err != nil && !apierrors.IsNotFound(err) {
			logging.Failure(fmt.Sprintf("[set] failed to delete pod %s/%s: %v", namespace, p.Name, err))
			return deleted, err
		}
		deleted = append(deleted, p.Name)
	}
	return deleted, nil
}
