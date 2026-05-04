// Package nodes computes which cluster nodes a ksvc is eligible to run
// on, given the ksvc's spec.template.spec.{nodeSelector, affinity,
// tolerations}. Read-only against the cluster.
//
// The package is intentionally small and stateless — every function is
// either pure or makes a single Get/List against the K8s API. Phase 2
// (per-node binary search) and Phase 3 (mutating webhook) of the
// multi-node refactor consume it; the online-stage placement logic
// (kept under the name "scheduling") will too.
//
// Known limitations — see multiple_nodes.md §11.6 for the full table.
package nodes

import (
	"context"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"nimbus/api/kubeconfig"
	"nimbus/api/nimbusevent"
)

// ErrNoMatching is returned when the target ksvc's scheduling constraints
// (nodeSelector + required nodeAffinity + tolerations) match zero
// schedulable nodes. Callers should treat this as a soft failure (the
// dev probably mis-typed a label) and retry on the next reconcile, not
// silently fall back to "all nodes".
var ErrNoMatching = errors.New("ksvc selector/affinity/tolerations match no schedulable nodes")

// Candidates returns the names of cluster nodes the ksvc targeted by ev
// is eligible to run on, sorted lexicographically. Read-only — no
// patches, no scheduling change.
//
// Filters applied, in order: Ready+!unschedulable, ksvc nodeSelector,
// ksvc nodeAffinity (required-only), ksvc tolerations vs node taints
// (NoSchedule + NoExecute only).
func Candidates(ctx context.Context, ev *nimbusevent.NimbusEvent) ([]string, error) {
	ksvcName := ev.Selector.MatchExpressions[0].Values[0]

	ksvcObj, err := kubeconfig.DYNCLIENT.Resource(kubeconfig.KSVC_GVR).
		Namespace(ev.Metadata.Namespace).
		Get(ctx, ksvcName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get ksvc %s/%s: %w", ev.Metadata.Namespace, ksvcName, err)
	}

	nodeSelector, nodeAffinity, tolerations, err := readKsvcScheduling(ksvcObj)
	if err != nil {
		return nil, fmt.Errorf("read ksvc scheduling spec: %w", err)
	}

	nodeList, err := kubeconfig.CLIENTSET.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}

	var matched []string
	for i := range nodeList.Items {
		n := &nodeList.Items[i]
		if !isReadyAndSchedulable(n) {
			continue
		}
		if !nodeSelectorMatches(n, nodeSelector) {
			continue
		}
		if !nodeAffinityMatches(n, nodeAffinity) {
			continue
		}
		if !tolerationsMatch(n, tolerations) {
			continue
		}
		matched = append(matched, n.Name)
	}
	sort.Strings(matched)

	if len(matched) == 0 {
		return nil, ErrNoMatching
	}
	return matched, nil
}

// readKsvcScheduling extracts spec.template.spec.{nodeSelector,
// affinity.nodeAffinity, tolerations} from an unstructured Knative
// service object. Returns (nil, nil, nil, nil) when none are present —
// a valid state meaning "the ksvc has no scheduling constraints".
func readKsvcScheduling(obj *unstructured.Unstructured) (map[string]string, *corev1.NodeAffinity, []corev1.Toleration, error) {
	templateSpec, found, err := unstructured.NestedMap(obj.Object, "spec", "template", "spec")
	if err != nil {
		return nil, nil, nil, err
	}
	if !found {
		return nil, nil, nil, nil
	}

	// Convert the template spec into a real PodSpec so we get typed
	// access without hand-walking every field. Only nodeSelector /
	// affinity / tolerations matter to us; the converter ignores
	// anything else gracefully.
	var podSpec corev1.PodSpec
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(templateSpec, &podSpec); err != nil {
		// If the conversion fails (e.g., a field type the converter
		// doesn't know about), fall back to manual nodeSelector
		// extraction so we still get the most common constraint.
		// Affinity + tolerations are dropped in that case.
		rawSel, _, _ := unstructured.NestedStringMap(obj.Object, "spec", "template", "spec", "nodeSelector")
		return rawSel, nil, nil, nil
	}

	var aff *corev1.NodeAffinity
	if podSpec.Affinity != nil {
		aff = podSpec.Affinity.NodeAffinity
	}
	return podSpec.NodeSelector, aff, podSpec.Tolerations, nil
}
