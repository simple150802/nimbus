package kubeapi

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"nimbus/api/logging"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// readKsvcApplyState reads the fields the apply helpers mutate, so those
// helpers can skip a no-op write when the ksvc already matches desired
// state (level-triggered reconcile: a converged tick issues no PATCH and
// logs nothing). cpuLimit/cpuRequest are container[0]'s values ("" if
// unset); selector is spec.template.spec.nodeSelector. found is false when
// the ksvc can't be read — callers then fall through to write (create-through).
func readKsvcApplyState(ctx context.Context, namespace, ksvcName string) (cpuLimit, cpuRequest string, selector map[string]string, found bool) {
	obj, err := DYNCLIENT.Resource(KSVC_GVR).Namespace(namespace).Get(ctx, ksvcName, metav1.GetOptions{})
	if err != nil {
		return "", "", nil, false
	}
	selector, _, _ = unstructured.NestedStringMap(obj.Object, "spec", "template", "spec", "nodeSelector")
	containers, _, _ := unstructured.NestedSlice(obj.Object, "spec", "template", "spec", "containers")
	if len(containers) > 0 {
		if c0, ok := containers[0].(map[string]interface{}); ok {
			cpuLimit, _, _ = unstructured.NestedString(c0, "resources", "limits", "cpu")
			cpuRequest, _, _ = unstructured.NestedString(c0, "resources", "requests", "cpu")
		}
	}
	return cpuLimit, cpuRequest, selector, true
}

// cpuEqual compares two CPU quantity strings semantically, so "1000m" and
// "1" count as equal. Falls back to string compare when either side can't
// be parsed (e.g. an empty current value). Used to avoid a spurious patch
// when the apiserver has normalized the stored quantity.
func cpuEqual(a, b string) bool {
	if a == b {
		return true
	}
	qa, ea := resource.ParseQuantity(a)
	qb, eb := resource.ParseQuantity(b)
	if ea != nil || eb != nil {
		return false
	}
	return qa.Cmp(qb) == 0
}

// selectorEqual reports whether two nodeSelector maps are exactly equal
// (same keys, same values). A desired-vs-current mismatch — including a
// leftover kubernetes.io/hostname pin on the current side — returns false
// so the caller re-asserts the pool selector.
func selectorEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

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

// PatchResourceLimits sets the CPU value on a Knative Service's
// container[0] for BOTH limits and requests, keeping them equal so the
// resulting pod is Guaranteed QoS — predictable bin-packing (scheduler
// reserves the full value, not a smaller floor) and the pod is never
// throttled below the converged CPU under contention. `add` ops are used
// instead of `replace` so the patch works whether or not the manifest
// already had a requests.cpu field. Logs the set event so the user can
// trace when the value actually hits the ksvc.
// Returns changed=true only when it actually issued a PATCH; a converged
// no-op returns (false, nil) so callers (the online reconciler) can stay
// silent when nothing moved.
func PatchResourceLimits(ctx context.Context, namespace, ksvcName, cpuLimit string) (changed bool, err error) {
	// No-op when the ksvc already carries this CPU on both requests and
	// limits — a converged reconcile tick issues no write and logs nothing.
	if curLimit, curReq, _, found := readKsvcApplyState(ctx, namespace, ksvcName); found &&
		cpuEqual(curLimit, cpuLimit) && cpuEqual(curReq, cpuLimit) {
		return false, nil
	}

	logging.Info(fmt.Sprintf("[set] ksvc cpu (request=limit) -> ns=%s ksvc=%s cpu=%s", namespace, ksvcName, cpuLimit))

	patchPayload := []map[string]interface{}{
		{
			"op":    "add",
			"path":  "/spec/template/spec/containers/0/resources/limits/cpu",
			"value": cpuLimit,
		},
		{
			"op":    "add",
			"path":  "/spec/template/spec/containers/0/resources/requests/cpu",
			"value": cpuLimit,
		},
	}

	payloadBytes, err := json.Marshal(patchPayload)
	if err != nil {
		return false, err
	}

	_, err = DYNCLIENT.Resource(KSVC_GVR).Namespace(namespace).Patch(
		ctx,
		ksvcName,
		types.JSONPatchType,
		payloadBytes,
		metav1.PatchOptions{},
	)
	return true, err
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

// ApplyKsvcSpec bundles the offline-phase apply mutations onto one ksvc
// into a single JSON-patch call: replace nodeSelector, then set
// requests.cpu and limits.cpu (kept equal for Guaranteed QoS). One
// apiserver round trip means the ksvc never lives in an intermediate
// state where the new nodeSelector has landed but the CPU hasn't (or
// vice versa) — a pod created mid-tick under the old composition is
// impossible.
//
// The StartupCPUBoost CR is a different resource entirely and stays a
// separate Create/Patch in the caller; cluster-level atomicity across
// both objects isn't achievable without a transactional admission
// pipeline.
//
// Both selector entries and runningCpu must be non-empty. `add` ops are
// used throughout so the call succeeds whether or not the target paths
// already exist on the ksvc.
//
// Returns changed=true only when it issued a PATCH; a converged no-op returns
// (false, nil) so callers (the online reconciler) stay silent when nothing moved.
func ApplyKsvcSpec(ctx context.Context, namespace, ksvcName string, selector map[string]string, runningCpu string) (changed bool, err error) {
	// No-op when nodeSelector and CPU (requests==limits) already match
	// desired — a converged reconcile tick issues no write and logs nothing.
	if curLimit, curReq, curSel, found := readKsvcApplyState(ctx, namespace, ksvcName); found &&
		cpuEqual(curLimit, runningCpu) && cpuEqual(curReq, runningCpu) && selectorEqual(curSel, selector) {
		return false, nil
	}

	logging.Info(fmt.Sprintf("[set] ksvc spec -> ns=%s ksvc=%s selector=%v cpu=%s",
		namespace, ksvcName, selector, runningCpu))

	selectorValue := map[string]interface{}{}
	for k, v := range selector {
		selectorValue[k] = v
	}

	patchPayload := []map[string]interface{}{
		{
			"op":    "add",
			"path":  "/spec/template/spec/nodeSelector",
			"value": selectorValue,
		},
		{
			"op":    "add",
			"path":  "/spec/template/spec/containers/0/resources/limits/cpu",
			"value": runningCpu,
		},
		{
			"op":    "add",
			"path":  "/spec/template/spec/containers/0/resources/requests/cpu",
			"value": runningCpu,
		},
	}

	payloadBytes, err := json.Marshal(patchPayload)
	if err != nil {
		return false, err
	}

	_, err = DYNCLIENT.Resource(KSVC_GVR).Namespace(namespace).Patch(
		ctx,
		ksvcName,
		types.JSONPatchType,
		payloadBytes,
		metav1.PatchOptions{},
	)
	return true, err
}

// PatchKsvcNodeSelector replaces the ksvc template's nodeSelector with the
// Nimbus-owned pool selector. This intentionally discards any previous manual
// selector on controlled ksvcs so one Nimbus means one placement pool.
//
// Prefer ApplyKsvcSpec when the caller is also patching CPU in the same
// tick — it bundles both mutations into one apiserver call. This
// function is kept for callers that need to set just the selector
// (e.g. the late-arriving ksvc path in StartKsvcWatcher).
func PatchKsvcNodeSelector(ctx context.Context, namespace, ksvcName string, selector map[string]string) error {
	// No-op when the ksvc's nodeSelector already equals the desired pool
	// selector — a converged reconcile tick issues no write and logs nothing.
	if _, _, curSel, found := readKsvcApplyState(ctx, namespace, ksvcName); found && selectorEqual(curSel, selector) {
		return nil
	}

	logging.Info(fmt.Sprintf("[set] ksvc nodeSelector -> ns=%s ksvc=%s selector=%v", namespace, ksvcName, selector))

	value := map[string]interface{}{}
	for k, v := range selector {
		value[k] = v
	}

	patchPayload := []map[string]interface{}{
		{
			"op":    "add",
			"path":  "/spec/template/spec/nodeSelector",
			"value": value,
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
