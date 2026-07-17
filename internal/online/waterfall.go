package online

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/resource"

	"nimbus/api/logging"
	"nimbus/api/nimbusevent"
)

// sidecarOverhead is the CPU (millicores) a Knative pod requests ON TOP of the
// user container — chiefly the queue-proxy sidecar (cluster default 25m). The
// waterfall's fit tests compare node headroom against the boost CPU (the user
// container), so without this term NIMBUS under-estimates the pod's real
// footprint and can admit a tier kube-scheduler then can't fit. Read once
// (after config/burst.env is loaded) from NIMBUS_SIDECAR_OVERHEAD_MILLI.
var (
	overheadOnce  sync.Once
	overheadMilli int64
)

func sidecarOverhead() int64 {
	overheadOnce.Do(func() {
		overheadMilli = 25 // queue-proxy default CPU request
		if v := os.Getenv("NIMBUS_SIDECAR_OVERHEAD_MILLI"); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
				overheadMilli = n
			} else {
				logging.Warning(fmt.Sprintf("[online] invalid NIMBUS_SIDECAR_OVERHEAD_MILLI=%q — using default 25", v))
			}
		}
	})
	return overheadMilli
}

// warmTierIsOpt reports whether the steady-state (revert) CPU should stay at
// c_opt_warm (the knee). Default is c_min_warm — set NIMBUS_WARM_TIER=opt to
// restore the original cold-only behaviour (warm locked at the knee: more
// latency margin, but more reserved CPU per ksvc → fewer fit before Pending).
var (
	warmTierOnce sync.Once
	warmTierOpt  bool
)

func warmTierIsOpt() bool {
	warmTierOnce.Do(func() {
		warmTierOpt = strings.EqualFold(os.Getenv("NIMBUS_WARM_TIER"), "opt")
	})
	return warmTierOpt
}

// warmValue returns the CPU the pod reverts to: c_min_warm by default (smallest
// meeting the warm SLO), c_opt_warm when NIMBUS_WARM_TIER=opt, and c_opt_warm as
// a fallback whenever c_min_warm is empty (warm SLO unset, or no probed sample
// met it). It is also the "warm floor" the admission checks enforce so the
// post-revert request fits the chosen node.
func warmValue(prof *nodeProfile) string {
	if warmTierIsOpt() || prof.MinRunning == "" {
		return prof.OptRunning
	}
	return prof.MinRunning
}

// decision is the waterfall outcome for one ksvc. The waterfall picks the
// COLD/boost value per tier (plus whether to pin the ksvc to a specific host);
// the steady-state WARM value is the same across all tiers — c_min_warm by
// default, or c_opt_warm under NIMBUS_WARM_TIER=opt (see warmValue). The
// ksvc.spec.template.spec.requests/limits carries that warm value; the
// StartupCPUBoost CR's cpu varies per tier.
//
// Tier semantics:
//
//	Tier 1 (TierCOpt)    — cold = c_opt_cold,  pool-wide.
//	Tier 2 (TierCMin)    — cold = c_min_cold,  pool-wide.
//	Tier 3 (TierBestFit) — cold = best.free_n, pinned to that host. degraded=true.
//	Pending              — no node has free_n ≥ warm; ksvc not admitted.
//
// node is "" when the placement is pool-wide; the hostname when Tier 3 pins.
type decision struct {
	tier     string // TierCOpt | TierCMin | TierBestFit | ""
	cold     string // boost CR cpu (varies per tier)
	warm     string // steady-state revert cpu (warmValue: c_min_warm by default)
	node     string // "" → pool-wide; hostname → pinned
	admitted bool
	degraded bool // true for Tier 3 (cold below c_min_cold)
}

// decideTier runs the 3-tier waterfall over live headroom + burst:
//
//	Tier 1: c_opt pool-wide   — usable_n ≥ c_opt_cold  on some node
//	Tier 2: c_min pool-wide   — usable_n ≥ max(c_min_cold, warm)
//	                            (the max guards against post-revert overcommit
//	                             when c_min_cold happens to be < warm)
//	Tier 3: best-fit pinned   — free_n  ≥ warm         on some node
//	Pending                   — otherwise
//
// usable_n = free_n × (1 − reserveRatio) applies the BURST reserve to Tiers 1
// and 2 (preserves headroom for the rest of a cold-start wave). Tier 3 uses
// raw free_n — we're already past conservation; admit whatever fits the warm
// floor on the best-fit node. The warm floor (warmValue: c_min_warm by default,
// c_opt_warm under NIMBUS_WARM_TIER=opt) guarantees the pod's post-revert
// request still fits the chosen host.
func decideTier(prof *nodeProfile, snap *poolSnapshot, mode BurstMode, reserve float64) decision {
	burst := mode == ModeBurst
	optCold, optColdOK := milliCPU(prof.OptStarting)
	minCold, minColdOK := milliCPU(prof.MinStarting)
	// warm is the steady-state CPU the pod reverts to AND the "warm floor" the
	// admission checks enforce. Default c_min_warm; NIMBUS_WARM_TIER=opt keeps
	// c_opt_warm (see warmValue). Same value across all tiers — the waterfall
	// varies only the cold/boost value per tier.
	warm := warmValue(prof)
	warmMilli, warmOK := milliCPU(warm)

	// The pod requests the boost CPU PLUS the queue-proxy sidecar, so every fit
	// test adds the sidecar overhead — otherwise NIMBUS admits a tier the
	// scheduler can't fit (it counts all containers). Computed once per call.
	oh := sidecarOverhead()
	maxUsable := snap.poolMaxUsable(reserve, burst)

	// Tier 1 — c_opt pool-wide.
	if optColdOK && maxUsable >= optCold+oh {
		return decision{tier: nimbusevent.TierCOpt, cold: prof.OptStarting, warm: warm, admitted: true}
	}

	// Tier 2 — c_min pool-wide. Require max(c_min_cold, warm) so the
	// post-boost-revert request still fits whichever node kube-scheduler picks.
	if minColdOK && warmOK {
		tier2Req := minCold
		if warmMilli > tier2Req {
			tier2Req = warmMilli
		}
		if maxUsable >= tier2Req+oh {
			return decision{tier: nimbusevent.TierCMin, cold: prof.MinStarting, warm: warm, admitted: true}
		}
	}

	// Tier 3 — best-fit pinned. Pick the node with the most raw free CPU
	// (most headroom margin against §8.2 capacity-snapshot drift). Cold value
	// is the node's free_n MINUS the sidecar overhead (so pod request = cold +
	// overhead fits free_n). Floor is warm (c_min_warm by default) so the
	// warm-side request is guaranteed to fit the chosen host post-revert.
	if warmOK {
		var best *nodeFree
		for _, n := range snap.nodes {
			if n.free < warmMilli+oh {
				continue
			}
			if best == nil || n.free > best.free {
				best = n
			}
		}
		if best != nil {
			return decision{
				tier:     nimbusevent.TierBestFit,
				cold:     formatMilli(best.free - oh),
				warm:     warm,
				node:     best.name,
				admitted: true,
				degraded: true,
			}
		}
	}

	return decision{admitted: false} // Pending — no node fits warm + overhead
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
