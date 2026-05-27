package online

import (
	"sort"

	"nimbus/api/nimbusevent"
)

// selectProfileNode returns the deterministic first node in ev.PerNodeResults
// that carries a complete profile — both StartingCpu and RunningCpu populated —
// chosen by ascending node name so the choice is stable across ticks and
// controller restarts.
//
// Returns ("", nil) when no node has a complete profile yet; the caller treats
// that as "Nimbus not saturated, skip this tick". Phase 1 uses exactly one node
// (the offline representative), but the sorted-first rule generalises cleanly if
// PerNodeResults ever carries more than one entry.
func selectProfileNode(ev *nimbusevent.NimbusEvent) (string, *nimbusevent.NodeResult) {
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
			return n, r
		}
	}
	return "", nil
}
