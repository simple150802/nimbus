package watcher

import (
	"context"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"nimbus/api/logging"
	"nimbus/api/nimbusevent"
)

// ErrNoMatchingNodes is returned when the target ksvc's nodeSelector +
// nodeAffinity match zero schedulable nodes. Callers should treat this
// as a soft failure (the dev probably mis-typed a label) and retry on
// the next reconcile, not silently fall back to "all nodes".
var ErrNoMatchingNodes = errors.New("ksvc selector/affinity matches no schedulable nodes")

// discoverCandidateNodes fills ev.CandidateNodes with the names of cluster
// nodes the target ksvc can be scheduled to. Read-only against the
// cluster — no patches, no scheduling change. The list is sorted so a
// later per-node loop is reproducible across reconciles.
func (nw *NimbusWatcher) discoverCandidateNodes(ctx context.Context, ev *nimbusevent.NimbusEvent) error {
	nodes, err := nw.computeCandidateNodes(ctx, ev)
	if err != nil {
		ev.CandidateNodes = nil
		return err
	}
	ev.CandidateNodes = nodes
	logging.Info(fmt.Sprintf("[nodes] %s/%s candidates: %v",
		ev.Metadata.Namespace, ev.Metadata.Name, nodes))
	return nil
}

// computeCandidateNodes is the pure logic — fetch ksvc, extract scheduling
// hints, list nodes, filter. Separated from discoverCandidateNodes so it
// can be exercised by tests without the side-effect of writing to ev.
//
// Filters applied, in order: Ready+!unschedulable, ksvc nodeSelector,
// ksvc nodeAffinity (required-only), ksvc tolerations vs node taints
// (NoSchedule + NoExecute only).
//
// Known limitations — see multiple_nodes.md §11.7 for the full table:
//   - Ignores podAffinity/podAntiAffinity, topologySpreadConstraints,
//     volume affinity, custom schedulerName, and resource fit.
//   - PreferNoSchedule taints + preferredDuringScheduling… affinity are
//     soft hints; we treat them as non-constraints. Matches kube-scheduler
//     semantics for hard eligibility.
//   - matchFields on nodeAffinity terms is unsupported (matchExpressions only).
//   - Gt / Lt operators on label values are unsupported (rare in app code).
func (nw *NimbusWatcher) computeCandidateNodes(ctx context.Context, ev *nimbusevent.NimbusEvent) ([]string, error) {
	ksvcName := ev.Selector.MatchExpressions[0].Values[0]

	ksvcObj, err := DYNCLIENT.Resource(KSVC_GVR).
		Namespace(ev.Metadata.Namespace).
		Get(ctx, ksvcName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get ksvc %s/%s: %w", ev.Metadata.Namespace, ksvcName, err)
	}

	nodeSelector, nodeAffinity, tolerations, err := readKsvcScheduling(ksvcObj)
	if err != nil {
		return nil, fmt.Errorf("read ksvc scheduling spec: %w", err)
	}

	nodes, err := CLIENTSET.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}

	var matched []string
	for i := range nodes.Items {
		n := &nodes.Items[i]
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
		return nil, ErrNoMatchingNodes
	}
	return matched, nil
}

// readKsvcScheduling extracts spec.template.spec.{nodeSelector, affinity.nodeAffinity,
// tolerations} from an unstructured Knative service object. Returns
// (nil, nil, nil, nil) when none are present — a valid state meaning
// "the ksvc has no scheduling constraints".
func readKsvcScheduling(obj *unstructured.Unstructured) (map[string]string, *corev1.NodeAffinity, []corev1.Toleration, error) {
	templateSpec, found, err := unstructured.NestedMap(obj.Object, "spec", "template", "spec")
	if err != nil {
		return nil, nil, nil, err
	}
	if !found {
		return nil, nil, nil, nil
	}

	// Convert the template spec into a real PodSpec so we get typed access
	// without hand-walking every field. Only nodeSelector / affinity /
	// tolerations matter to us; the converter ignores anything else.
	var podSpec corev1.PodSpec
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(templateSpec, &podSpec); err != nil {
		// If the conversion fails (e.g. a field type the converter doesn't
		// know about), fall back to manual extraction so we at least get
		// the nodeSelector. Affinity + tolerations are dropped in that case.
		rawSel, _, _ := unstructured.NestedStringMap(obj.Object, "spec", "template", "spec", "nodeSelector")
		return rawSel, nil, nil, nil
	}

	var aff *corev1.NodeAffinity
	if podSpec.Affinity != nil {
		aff = podSpec.Affinity.NodeAffinity
	}
	return podSpec.NodeSelector, aff, podSpec.Tolerations, nil
}

// isReadyAndSchedulable returns true iff the node accepts new pods. We
// reject nodes that are cordoned (spec.unschedulable=true) or whose
// Ready condition isn't True.
func isReadyAndSchedulable(node *corev1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// nodeSelectorMatches: every key in sel must be present on node.Labels
// with the same value. Empty/nil selector matches all nodes.
func nodeSelectorMatches(node *corev1.Node, sel map[string]string) bool {
	for k, v := range sel {
		if node.Labels[k] != v {
			return false
		}
	}
	return true
}

// nodeAffinityMatches: nil affinity → matches all. Otherwise, satisfy
// requiredDuringSchedulingIgnoredDuringExecution. Per Kubernetes
// semantics, the term list is OR'd — any term can match. Within a term,
// matchExpressions are AND'd.
//
// preferredDuringScheduling…IgnoredDuringExecution is intentionally
// ignored — for profiling we only honor hard constraints.
func nodeAffinityMatches(node *corev1.Node, aff *corev1.NodeAffinity) bool {
	if aff == nil || aff.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return true
	}
	terms := aff.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) == 0 {
		return true
	}
	for _, t := range terms {
		if termMatches(node, t) {
			return true
		}
	}
	return false
}

// termMatches: every matchExpression in the term must be satisfied (AND).
// matchFields exists in the API but is rarely used by app developers; we
// ignore it for v1 to keep the implementation compact.
func termMatches(node *corev1.Node, term corev1.NodeSelectorTerm) bool {
	for _, e := range term.MatchExpressions {
		if !exprMatches(node, e) {
			return false
		}
	}
	return true
}

// exprMatches: evaluate a single In/NotIn/Exists/DoesNotExist requirement
// against the node's labels. Gt/Lt are numeric and rare in real-world
// affinity; we conservatively return false for them rather than guess.
func exprMatches(node *corev1.Node, e corev1.NodeSelectorRequirement) bool {
	val, has := node.Labels[e.Key]
	switch e.Operator {
	case corev1.NodeSelectorOpIn:
		if !has {
			return false
		}
		for _, v := range e.Values {
			if v == val {
				return true
			}
		}
		return false
	case corev1.NodeSelectorOpNotIn:
		if !has {
			return true
		}
		for _, v := range e.Values {
			if v == val {
				return false
			}
		}
		return true
	case corev1.NodeSelectorOpExists:
		return has
	case corev1.NodeSelectorOpDoesNotExist:
		return !has
	default:
		// Gt/Lt: numeric comparison; not supported in v1.
		return false
	}
}

// tolerationsMatch returns true iff every NoSchedule / NoExecute taint on
// the node is matched by at least one toleration in tols. PreferNoSchedule
// is treated as a soft hint and ignored — same as kube-scheduler when
// deciding hard eligibility.
//
// This is what catches the "master node has node-role.kubernetes.io/control-plane:NoSchedule
// but the user's ksvc doesn't tolerate it" case.
func tolerationsMatch(node *corev1.Node, tols []corev1.Toleration) bool {
	for i := range node.Spec.Taints {
		t := &node.Spec.Taints[i]
		if t.Effect != corev1.TaintEffectNoSchedule && t.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		if !anyTolerationMatches(t, tols) {
			return false
		}
	}
	return true
}

// anyTolerationMatches: does any toleration in tols satisfy this taint?
// A toleration matches a taint when:
//   - tol.Effect is empty (matches all effects) OR equal to taint.Effect, AND
//   - operator is Exists with key empty (match-all) OR matching key, OR
//   - operator is Equal (default) with matching key AND value.
func anyTolerationMatches(taint *corev1.Taint, tols []corev1.Toleration) bool {
	for _, tol := range tols {
		if tol.Effect != "" && tol.Effect != taint.Effect {
			continue
		}
		switch tol.Operator {
		case "", corev1.TolerationOpEqual:
			if tol.Key == taint.Key && tol.Value == taint.Value {
				return true
			}
		case corev1.TolerationOpExists:
			if tol.Key == "" || tol.Key == taint.Key {
				return true
			}
		}
	}
	return false
}
