// Package online is the NIMBUS online stage: it consumes the completed offline
// profile and assigns CPU/tier/placement to the pre-created ksvcs listed in a
// Nimbus's selector.matchExpressions[0].values.
//
// Phase 1 (this code) is a polling reconciler that assigns the c_opt tier to
// every listed ksvc — no headroom math, no hostname pinning, no burst detector
// (Phase 3) and no KPA RPC (Phase 4). It runs in its own goroutine, started
// from cmd/main.go, and is deliberately decoupled from the offline worker:
//
//   - reads ONLY completed Nimbus snapshots via watcher.ListCompleted();
//   - writes ONLY .status.online (never .status.perNode);
//   - never touches the offline queue, the binary search, export, or preload.
//
// Import discipline: this package must not import api/algorithm,
// internal/export, or internal/preMeasured — see online_implementation_plan.md.
package online

import (
	"context"
	"fmt"
	"sort"
	"time"

	"nimbus/api/logging"
	"nimbus/internal/watcher"
)

// tickInterval is the polling cadence. The offline worker uses the same 2s
// beat; the online controller is independent of it.
const tickInterval = 2 * time.Second

// StartController runs the Phase 1 online reconciler until ctx is cancelled.
// Every tickInterval it snapshots all completed Nimbuses and reconciles each
// one to the c_opt tier. Blocking; intended to be launched as `go
// online.StartController(ctx, nw)`.
func StartController(ctx context.Context, nw *watcher.NimbusWatcher) {
	logging.Info(fmt.Sprintf("[online] event=controller_start interval=%s action=start", tickInterval))

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	// last carries each Nimbus's previous outcome across ticks so a converged
	// reconcile writes nothing and logs nothing. Owned here, single-goroutine.
	last := make(map[string]lastOutcome)

	for {
		select {
		case <-ctx.Done():
			logging.Info("[online] event=controller_stop action=stop reason=context_cancelled")
			return
		case <-ticker.C:
			runTick(ctx, nw, last)
		}
	}
}

// runTick reconciles every completed Nimbus exactly once. The snapshot list is
// sorted by namespace/name so duplicate-ksvc ownership resolves deterministically
// across ticks (the alphabetically-first Nimbus always wins a contested ksvc).
//
// Level-triggered: the tick_complete summary is logged only when at least one
// Nimbus changed this tick, so a fully-converged cluster produces zero output.
func runTick(ctx context.Context, nw *watcher.NimbusWatcher, last map[string]lastOutcome) {
	completed := nw.ListCompleted()
	if len(completed) == 0 {
		return
	}

	start := time.Now()
	sort.Slice(completed, func(i, j int) bool {
		a := completed[i].Metadata.Namespace + "/" + completed[i].Metadata.Name
		b := completed[j].Metadata.Namespace + "/" + completed[j].Metadata.Name
		return a < b
	})

	// claimed maps "ns/ksvc" -> owning Nimbus name for this tick, so two
	// Nimbuses listing the same ksvc don't fight over it.
	claimed := make(map[string]string)
	reconciled := 0
	totalAssignments := 0
	tickChanged := false
	for _, ev := range completed {
		n, changed := ReconcileOne(ctx, ev, claimed, last)
		if n >= 0 {
			reconciled++
			totalAssignments += n
		}
		if changed {
			tickChanged = true
		}
	}

	if tickChanged {
		logging.Info(fmt.Sprintf("[online][tick] event=tick_complete completed=%d reconciled=%d assignments=%d duration_ms=%d",
			len(completed), reconciled, totalAssignments, time.Since(start).Milliseconds()))
	}
}
