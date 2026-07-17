package online

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"nimbus/api/kubeconfig"
	"nimbus/api/logging"
)

// logSnapshotEnabled gates the per-snapshot headroom log (NIMBUS's own view of
// free_n per node, the basis for every waterfall decision). OFF by default —
// buildPoolSnapshot runs on every /decide and every 2s reconcile tick, so
// always-on would flood nimbus.log. Set NIMBUS_LOG_SNAPSHOT=1 during an
// experiment to cross-check NIMBUS's math against the external sample_resources.py.
var (
	logSnapOnce sync.Once
	logSnapOn   bool
)

func logSnapshotEnabled() bool {
	logSnapOnce.Do(func() {
		v := os.Getenv("NIMBUS_LOG_SNAPSHOT")
		logSnapOn = v == "1" || strings.EqualFold(v, "true")
	})
	return logSnapOn
}

// nodeFree is one candidate pool node's live headroom. All CPU values are
// millicores. free is the smaller of two views (see buildPoolSnapshot) and is
// mutated within a single reconcile tick by deductCold as ksvcs are placed
// (online.md §5.5).
type nodeFree struct {
	name           string
	allocCPU       int64 // node.status.allocatable.cpu
	budget         int64 // (NIMBUS_BUDGET_PCT/100) × allocCPU — serverless soft cap
	serverlessUsed int64 // Σ Knative pod requests in ns (soft-budget accounting)
	allUsed        int64 // Σ ALL pod requests bound to the node (physical accounting)
	free           int64 // max(0, min(budget − serverlessUsed, allocCPU − allUsed))
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
// the committed pod requests, and computes free_n per node as the SMALLER of
// two views:
//
//	soft  = budget − serverlessUsed   (don't exceed the serverless soft cap)
//	phys  = allocatable − allUsed      (don't exceed what kube-scheduler will fit)
//
// The phys term closes the gap where a node looks free to NIMBUS but is
// actually filled by non-serverless workloads (system pods, other namespaces),
// which the scheduler's NodeResourcesFit counts and NIMBUS previously ignored.
// One nodes.list + one pods.list — no persistent ledger (online.md §5.5).
// Returns an error when the pool selector matches no Ready nodes (caller treats
// it as "can't place").
func buildPoolSnapshot(ctx context.Context, selector map[string]string, ns string, pct int) (*poolSnapshot, error) {
	nodes, err := resolvePoolNodes(ctx, selector, pct)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no Ready schedulable nodes match pool selector %v", selector)
	}
	if err := computeUsed(ctx, ns, nodes); err != nil {
		return nil, err
	}

	snap := &poolSnapshot{nodes: nodes, byName: make(map[string]*nodeFree, len(nodes))}
	for _, n := range nodes {
		n.free = maxZero(minInt64(n.budget-n.serverlessUsed, n.allocCPU-n.allUsed))
		snap.byName[n.name] = n
	}

	if logSnapshotEnabled() {
		parts := make([]string, 0, len(snap.nodes))
		for _, n := range snap.nodes {
			parts = append(parts, fmt.Sprintf("%s{free=%dm alloc=%dm budget=%dm svcUsed=%dm allUsed=%dm}",
				n.name, n.free, n.allocCPU, n.budget, n.serverlessUsed, n.allUsed))
		}
		logging.Info(fmt.Sprintf("[online][snapshot] event=pool_headroom ns=%s nodes=%d %s",
			ns, len(snap.nodes), strings.Join(parts, " ")))
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

// computeUsed sums CPU requests per candidate node into two buckets (Running or
// Pending-but-bound pods only):
//
//   - allUsed: EVERY pod bound to the node, across all namespaces — the physical
//     view kube-scheduler enforces via NodeResourcesFit (allocatable − Σ requests).
//   - serverlessUsed: Knative pods in ns — the serverless soft-budget view.
//
// buildPoolSnapshot then takes free = min(budget − serverlessUsed,
// allocatable − allUsed), so NIMBUS never reads a node as free that the
// scheduler would consider full (e.g. filled by system/other-namespace pods).
//
// A pods.list error is propagated (fail-closed): buildPoolSnapshot then aborts
// and the caller treats the tick as "can't place". The old best-effort
// behaviour left used=0 on a list error, which reads a full node as empty and
// admits on top of it (overcommit) — worse than briefly deferring the decision.
func computeUsed(ctx context.Context, ns string, nodes []*nodeFree) error {
	idx := make(map[string]*nodeFree, len(nodes))
	for _, n := range nodes {
		idx[n.name] = n
	}
	pods, err := kubeconfig.CLIENTSET.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list pods for headroom: %w", err)
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
		var podCPU int64
		for c := range p.Spec.Containers {
			podCPU += p.Spec.Containers[c].Resources.Requests.Cpu().MilliValue()
		}
		nf.allUsed += podCPU
		if p.Namespace == ns {
			if _, isKnative := p.Labels["serving.knative.dev/service"]; isKnative {
				nf.serverlessUsed += podCPU
			}
		}
	}
	return nil
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

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
