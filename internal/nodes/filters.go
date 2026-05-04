package nodes

import (
	corev1 "k8s.io/api/core/v1"
)

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
