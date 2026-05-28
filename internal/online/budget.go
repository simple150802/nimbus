package online

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"nimbus/api/kubeconfig"
)

// nodeFree is one candidate pool node's live headroom for the serverless
// namespace. All CPU values are millicores. free is mutated within a single
// reconcile tick by deductCold as ksvcs are placed (online.md §5.5).
type nodeFree struct {
	name     string
	allocCPU int64 // node.status.allocatable.cpu
	budget   int64 // (NIMBUS_BUDGET_PCT/100) × allocCPU
	used     int64 // Σ serverless-knative pod requests bound to this node
	free     int64 // max(0, budget − used)
}

// usable returns the free CPU the waterfall may spend on this node. In BURST
// mode an extra reserveRatio of free is held back for the rest of the wave.
func (n *nodeFree) usable(reserve float64, burst bool) int64 {
	if burst {
		return int64(float64(n.free) * (1 - reserve))
	}
	return n.free
}

// poolSnapshot is the per-tick headroom view of one Nimbus's node pool.
type poolSnapshot struct {
	nodes  []*nodeFree
	byName map[string]*nodeFree
}

// poolMaxUsable is the largest usable free across the pool — the test for
// whether a tier fits anywhere pool-wide.
func (s *poolSnapshot) poolMaxUsable(reserve float64, burst bool) int64 {
	var mx int64
	for _, n := range s.nodes {
		if u := n.usable(reserve, burst); u > mx {
			mx = u
		}
	}
	return mx
}

// buildPoolSnapshot resolves the Nimbus pool, reads live allocatable CPU and
// the serverless namespace's committed Knative pod requests, and computes
// free_n per node. One nodes.list + one pods.list — no persistent ledger
// (online.md §5.5). Returns an error when the pool selector matches no Ready
// nodes (caller treats it as "can't place").
func buildPoolSnapshot(ctx context.Context, selector map[string]string, ns string, pct int) (*poolSnapshot, error) {
	nodes, err := resolvePoolNodes(ctx, selector, pct)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no Ready schedulable nodes match pool selector %v", selector)
	}
	computeUsed(ctx, ns, nodes)

	snap := &poolSnapshot{nodes: nodes, byName: make(map[string]*nodeFree, len(nodes))}
	for _, n := range nodes {
		n.free = maxZero(n.budget - n.used)
		snap.byName[n.name] = n
	}
	return snap, nil
}

// resolvePoolNodes lists Ready+schedulable nodes whose labels match every
// key/value in the pool selector, with budget_n = pct% of allocatable.cpu.
// Sorted by name for determinism. Replicated locally (not imported from
// internal/watcher) to keep the offline/online package boundary clean.
func resolvePoolNodes(ctx context.Context, selector map[string]string, pct int) ([]*nodeFree, error) {
	list, err := kubeconfig.CLIENTSET.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	var out []*nodeFree
	for i := range list.Items {
		nd := &list.Items[i]
		if !nodeReadySchedulable(nd) || !labelsMatch(nd.Labels, selector) {
			continue
		}
		alloc := nd.Status.Allocatable.Cpu().MilliValue()
		out = append(out, &nodeFree{
			name:     nd.Name,
			allocCPU: alloc,
			budget:   alloc * int64(pct) / 100,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out, nil
}

// computeUsed sums the CPU requests of serverless-namespace Knative pods bound
// to each candidate node (Running or Pending-but-bound). Best-effort: a list
// error leaves used at zero (the next tick retries) rather than aborting.
func computeUsed(ctx context.Context, ns string, nodes []*nodeFree) {
	idx := make(map[string]*nodeFree, len(nodes))
	for _, n := range nodes {
		idx[n.name] = n
	}
	pods, err := kubeconfig.CLIENTSET.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "serving.knative.dev/service",
	})
	if err != nil {
		return
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Spec.NodeName == "" {
			continue // unscheduled — reserves nothing yet
		}
		if p.Status.Phase != corev1.PodRunning && p.Status.Phase != corev1.PodPending {
			continue
		}
		nf := idx[p.Spec.NodeName]
		if nf == nil {
			continue // pod on a node outside this pool
		}
		for c := range p.Spec.Containers {
			nf.used += p.Spec.Containers[c].Resources.Requests.Cpu().MilliValue()
		}
	}
}

func nodeReadySchedulable(n *corev1.Node) bool {
	if n.Spec.Unschedulable {
		return false
	}
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// labelsMatch reports whether every key/value in selector is present on labels.
func labelsMatch(labels, selector map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

func maxZero(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}
