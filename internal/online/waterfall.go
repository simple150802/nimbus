package online

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	"nimbus/api/nimbusevent"
)

// decision is the waterfall outcome for one ksvc. The thesis scope is
// cold-start optimization only: the steady-state (warm) side is locked to
// c_opt_warm on every tier — so the only quantity the waterfall picks per
// tier is the COLD/boost value, plus whether to pin the ksvc to a specific
// host. The ksvc.spec.template.spec.requests/limits never changes from the
// offline-written c_opt_warm; the StartupCPUBoost CR's cpu varies per tier.
//
// Tier semantics:
//
//	Tier 1 (TierCOpt)    — cold = c_opt_cold,  pool-wide.
//	Tier 2 (TierCMin)    — cold = c_min_cold,  pool-wide.
//	Tier 3 (TierBestFit) — cold = best.free_n, pinned to that host. degraded=true.
//	Pending              — no node has free_n ≥ c_opt_warm; ksvc not admitted.
//
// node is "" when the placement is pool-wide; the hostname when Tier 3 pins.
type decision struct {
	tier     string // TierCOpt | TierCMin | TierBestFit | ""
	cold     string // boost CR cpu (varies per tier)
	warm     string // ALWAYS prof.OptRunning (c_opt_warm)
	node     string // "" → pool-wide; hostname → pinned
	admitted bool
	degraded bool // true for Tier 3 (cold below c_min_cold)
}

// decideTier runs the cold-only 3-tier waterfall over live headroom + burst:
//
//	Tier 1: c_opt pool-wide   — usable_n ≥ c_opt_cold  on some node
//	Tier 2: c_min pool-wide   — usable_n ≥ max(c_min_cold, c_opt_warm)
//	                            (the max guards against post-revert overcommit
//	                             when c_min_cold happens to be < c_opt_warm)
//	Tier 3: best-fit pinned   — free_n  ≥ c_opt_warm   on some node
//	Pending                   — otherwise
//
// usable_n = free_n × (1 − reserveRatio) applies the BURST reserve to Tiers 1
// and 2 (preserves headroom for the rest of a cold-start wave). Tier 3 uses
// raw free_n — we're already past conservation; admit whatever fits the warm
// floor on the best-fit node. The warm floor (c_opt_warm) guarantees that the
// pod's post-revert request still fits the chosen host.
func decideTier(prof *nodeProfile, snap *poolSnapshot, mode BurstMode, reserve float64) decision {
	burst := mode == ModeBurst
	optCold, optColdOK := milliCPU(prof.OptStarting)
	optWarm, optWarmOK := milliCPU(prof.OptRunning)
	minCold, minColdOK := milliCPU(prof.MinStarting)
	warm := prof.OptRunning // locked across all tiers (thesis: cold-only optimization)

	// Tier 1 — c_opt pool-wide.
	if optColdOK && snap.poolMaxUsable(reserve, burst) >= optCold {
		return decision{tier: nimbusevent.TierCOpt, cold: prof.OptStarting, warm: warm, admitted: true}
	}

	// Tier 2 — c_min pool-wide. Require max(c_min_cold, c_opt_warm) so the
	// post-boost-revert request still fits whichever node kube-scheduler picks.
	if minColdOK && optWarmOK {
		tier2Req := minCold
		if optWarm > tier2Req {
			tier2Req = optWarm
		}
		if snap.poolMaxUsable(reserve, burst) >= tier2Req {
			return decision{tier: nimbusevent.TierCMin, cold: prof.MinStarting, warm: warm, admitted: true}
		}
	}

	// Tier 3 — best-fit pinned. Pick the node with the most raw free CPU
	// (most headroom margin against §8.2 capacity-snapshot drift). Cold value
	// is that node's full free_n. Floor is c_opt_warm so the warm-side request
	// is guaranteed to fit the chosen host post-revert.
	if optWarmOK {
		var best *nodeFree
		for _, n := range snap.nodes {
			if n.free < optWarm {
				continue
			}
			if best == nil || n.free > best.free {
				best = n
			}
		}
		if best != nil {
			return decision{
				tier:     nimbusevent.TierBestFit,
				cold:     formatMilli(best.free),
				warm:     warm,
				node:     best.name,
				admitted: true,
				degraded: true,
			}
		}
	}

	return decision{admitted: false} // Pending — no node has free_n ≥ c_opt_warm
}

// deductCold subtracts an admitted decision's COLD CPU from the snapshot so
// later ksvcs in the same tick see reduced headroom. Pinned (Tier 3) deducts
// from the pinned node; pool-wide tiers deduct from the current max-free node
// (the presumed kube-scheduler landing spot).
func deductCold(snap *poolSnapshot, d decision) {
	if !d.admitted {
		return
	}
	val, ok := milliCPU(d.cold)
	if !ok {
		return
	}
	if d.node != "" {
		if nf := snap.byName[d.node]; nf != nil {
			nf.free = maxZero(nf.free - val)
		}
		return
	}
	var best *nodeFree
	for _, n := range snap.nodes {
		if best == nil || n.free > best.free {
			best = n
		}
	}
	if best != nil {
		best.free = maxZero(best.free - val)
	}
}

// buildSelector returns the nodeSelector for an admitted decision: the pool
// selector verbatim, plus a kubernetes.io/hostname key when Tier 3 pins. The
// caller passes the result to ApplyKsvcSpec, which replaces the ksvc's
// nodeSelector wholesale — so dropping the hostname key on a tier transition
// back to pool-wide happens automatically.
func buildSelector(pool map[string]string, d decision) map[string]string {
	s := make(map[string]string, len(pool)+1)
	for k, v := range pool {
		s[k] = v
	}
	if d.node != "" {
		s["kubernetes.io/hostname"] = d.node
	}
	return s
}

// poolLabelValue is the human-readable "node" shown on a pool-wide status row
// (Tier 1/2). For Tier 3 the status carries the hostname instead. For a
// single-key pool selector the value (e.g. "serverless") is used; for a
// multi-key selector it's a sorted "k=v,k=v" join.
func poolLabelValue(sel map[string]string) string {
	if len(sel) == 1 {
		for _, v := range sel {
			return v
		}
	}
	parts := make([]string, 0, len(sel))
	for k, v := range sel {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// milliCPU parses a k8s CPU quantity to millicores. ok=false for "" or an
// unparseable value — callers treat that tier as unavailable.
func milliCPU(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0, false
	}
	return q.MilliValue(), true
}

// formatMilli renders a millicore value as a k8s CPU quantity string ("500m").
// Used for Tier 3 where the cold value is dynamic (a specific node's free_n).
func formatMilli(milli int64) string {
	return fmt.Sprintf("%dm", milli)
}
