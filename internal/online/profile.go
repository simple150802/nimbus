package online

import (
	"sort"

	"nimbus/api/nimbusevent"
)

// nodeProfile is the online stage's view of one node's offline profile. It
// carries both CPU tiers the waterfall needs:
//
//   - Opt* — c_opt, the converged knee of the latency curve. Phase 1 applies
//     this tier unconditionally.
//   - Min* — c_min, the smallest probed CPU still meeting the SLO. Extracted
//     here so the Phase 3 headroom waterfall can fall back to it when c_opt
//     doesn't fit. May be "" when spec.acceptableResponseTime was unset or no
//     probed sample met the budget — Phase 3 treats an empty Min* as "no
//     middle tier for this phase".
//
// All values are read directly from the offline-produced NodeResult; the
// online stage never recomputes them (offline owns measurement). Because
// thesis preload keeps SLO/metric fixed across export↔import, the imported
// cMin values are used as-is.
type nodeProfile struct {
	OptStarting string // c_opt cold — NodeResult.StartingCpu
	OptRunning  string // c_opt warm — NodeResult.RunningCpu
	MinStarting string // c_min cold — NodeResult.CMinStarting
	MinRunning  string // c_min warm — NodeResult.CMinRunning
}

// selectProfileNode returns the deterministic first node in ev.PerNodeResults
// that carries a complete profile — both StartingCpu and RunningCpu populated —
// chosen by ascending node name so the choice is stable across ticks and
// controller restarts, along with that node's nodeProfile (c_opt + c_min).
//
// Returns ("", nil) when no node has a complete profile yet; the caller treats
// that as "Nimbus not saturated, skip this tick". Phase 1 uses exactly one node
// (the offline representative), but the sorted-first rule generalises cleanly if
// PerNodeResults ever carries more than one entry. Completeness is gated on the
// c_opt pair only — c_min is optional (depends on spec.acceptableResponseTime).
func selectProfileNode(ev *nimbusevent.NimbusEvent) (string, *nodeProfile) {
	if len(ev.PerNodeResults) == 0 {
		return "", nil
	}
	names := make([]string, 0, len(ev.PerNodeResults))
	for n := range ev.PerNodeResults {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		r := ev.PerNodeResults[n]
		if r != nil && r.StartingCpu != "" && r.RunningCpu != "" {
			return n, &nodeProfile{
				OptStarting: r.StartingCpu,
				OptRunning:  r.RunningCpu,
				MinStarting: r.CMinStarting,
				MinRunning:  r.CMinRunning,
			}
		}
	}
	return "", nil
}
