package watcher

import (
	"context"
	"fmt"
	"time"

	"nimbus/api/algorithm"
	"nimbus/api/kubeapi"
	"nimbus/api/logging"
	"nimbus/api/nimbusevent"
	"nimbus/internal/export"
)

// runMultiNodeSearch loops the binary search over each candidate node.
// Each iteration pins the ksvc to the node so all probed pods land there;
// each new pin overwrites the previous, so a single deferred unpin at the
// end is enough to restore the user's scheduling. Inner skip: a node whose
// PerNodeResults entry is already fully saturated (both phases done from a
// previous run, recovered from .status) is skipped — same idea as the old
// flat-Saturated fast path, but at per-node granularity. On any per-node
// error the loop aborts; partial PerNodeResults stay on the event so the
// next reconcile can resume from the saturated nodes.
func (nw *NimbusWatcher) runMultiNodeSearch(ctx context.Context, current *nimbusevent.NimbusEvent) error {
	ns := current.Metadata.Namespace
	ksvc := current.Selector.MatchExpressions[0].Values[0]

	// Initialise sample-export filesystem layout if the Nimbus opted in via
	// spec.export.dir. Failure is non-fatal — search proceeds with no export.
	runStartedAt := time.Now()
	if current.Spec.Export != nil && current.Spec.Export.Dir != "" {
		runRoot, err := export.InitRunDir(current.Spec.Export.Dir, runStartedAt)
		if err != nil {
			logging.Warning("[export] InitRunDir failed; samples will not be persisted to disk:", err)
		} else {
			current.ExportRoot = runRoot
			if err := export.WriteMeta(runRoot, current, current.CandidateNodes, runStartedAt); err != nil {
				logging.Warning("[export] WriteMeta failed:", err)
			}
		}
	}

	defer func() {
		if err := kubeapi.UnpinKsvc(ctx, ns, ksvc); err != nil {
			logging.Warning("[nodes] failed to unpin ksvc after search:", err)
		}
	}()

	for _, node := range current.CandidateNodes {
		if r := current.PerNodeResults[node]; r != nil && r.StartingSaturated && r.RunningSaturated {
			logging.Info(fmt.Sprintf("[nodes] node=%s already saturated (starting=%s running=%s) — skipping",
				node, r.StartingCpu, r.RunningCpu))
			continue
		}

		if err := kubeapi.PinKsvcToNode(ctx, ns, ksvc, node); err != nil {
			return fmt.Errorf("pin to %s: %w", node, err)
		}
		logging.Stage(fmt.Sprintf("[nodes] BinarySearch on node=%s", node))
		if _, err := algorithm.BinarySearch(ctx, current, node); err != nil {
			return fmt.Errorf("BinarySearch on %s: %w", node, err)
		}

		// Write per-node result.json now (not at end-of-loop) so partial
		// progress survives a mid-loop crash. Non-fatal on failure.
		if current.ExportRoot != "" {
			if err := export.WriteResult(current.ExportRoot, node, current.PerNodeResults[node], time.Now()); err != nil {
				logging.Warning(fmt.Sprintf("[export] WriteResult(%s) failed: %v", node, err))
			}
		}
	}
	return nil
}

// loadPerNodeFromStatus copies whatever's in .status.perNode into the
// runtime PerNodeResults map and recomputes the per-phase Saturated flags
// from the emptiness of the persisted CPU strings. Always called after
// discovery so the worker can see partial progress from a previous run.
// Also sets current.AllSaturated via recomputeAllSaturated.
func loadPerNodeFromStatus(current *nimbusevent.NimbusEvent) {
	current.PerNodeResults = make(map[string]*nimbusevent.NodeResult, len(current.CandidateNodes))
	for _, node := range current.CandidateNodes {
		r := current.Status.PerNode[node]
		// Defensive copies of the sample slices so the runtime view doesn't
		// share backing storage with current.Status.PerNode (which the watch
		// decoder still owns and may reuse on the next event).
		var cold, warm []nimbusevent.SamplePoint
		if len(r.ColdRtSamples) > 0 {
			cold = append([]nimbusevent.SamplePoint(nil), r.ColdRtSamples...)
		}
		if len(r.WarmRtSamples) > 0 {
			warm = append([]nimbusevent.SamplePoint(nil), r.WarmRtSamples...)
		}
		current.PerNodeResults[node] = &nimbusevent.NodeResult{
			StartingCpu:       r.StartingCpu,
			RunningCpu:        r.RunningCpu,
			ColdRtSamples:     cold,
			WarmRtSamples:     warm,
			StartingSaturated: r.StartingCpu != "",
			RunningSaturated:  r.RunningCpu != "",
		}
	}
	recomputeAllSaturated(current)
}

// recomputeAllSaturated maintains the invariant that current.AllSaturated
// is true iff every candidate node has both StartingSaturated and
// RunningSaturated. Called after loading from status and after the slow
// path completes so the outer flag never lags the inner state.
func recomputeAllSaturated(current *nimbusevent.NimbusEvent) {
	if len(current.CandidateNodes) == 0 {
		current.AllSaturated = false
		return
	}
	for _, node := range current.CandidateNodes {
		r := current.PerNodeResults[node]
		if r == nil || !r.StartingSaturated || !r.RunningSaturated {
			current.AllSaturated = false
			return
		}
	}
	current.AllSaturated = true
}
