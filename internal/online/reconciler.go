package online

import (
	"context"
	"fmt"
	"reflect"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"nimbus/api/kubeapi"
	"nimbus/api/kubeconfig"
	"nimbus/api/logging"
	"nimbus/api/nimbusevent"
)

// lastOutcome is the previous tick's result for one Nimbus, used to suppress
// duplicate logs and status writes when nothing changed. Exactly one of the
// two fields is meaningful: online is set when the last tick produced an
// assignment status; skipReason is set when the last tick skipped (no
// selector / no complete profile). Owned by the controller across ticks and
// passed into ReconcileOne; single-goroutine, no locking.
type lastOutcome struct {
	online     *nimbusevent.OnlineStatus
	skipReason string
}

// noteSkip records a skip and logs it only on entry to the skip state — a
// Nimbus that stays unsaturated is logged once, not every 2s. Returns whether
// this tick was a change (i.e. the skip state was newly entered).
func noteSkip(last map[string]lastOutcome, key, reason string, logFn func()) bool {
	if prev := last[key]; prev.online == nil && prev.skipReason == reason {
		return false
	}
	logFn()
	last[key] = lastOutcome{skipReason: reason}
	return true
}

// enforceOfflineBootstrap re-asserts the offline-applied state on every managed
// ksvc — pool selector, c_opt_warm CPU, max-scale=1, boost CR at c_opt_cold.
// Used when spec.online.enabled=false: the adaptive waterfall and status.online
// write are skipped, but drift correction is not. Without this an "offline only"
// baseline silently breaks when anything edits a managed ksvc between Nimbus CR
// re-applies.
//
// Returns true iff at least one apiserver write happened this call. Both
// helpers (ApplyKsvcSpec, CreateStartupCPUBoost) are no-op-aware, so a
// converged tick issues no writes and no log lines.
//
// claimed is the per-tick "ns/ksvc -> owning Nimbus" map (same semantics as
// the waterfall path) so two Nimbuses can't fight over one ksvc.
func enforceOfflineBootstrap(ctx context.Context, ev *nimbusevent.NimbusEvent, claimed map[string]string) bool {
	ns := ev.Metadata.Namespace
	nimbus := ev.Metadata.Name
	selector := ev.Spec.Placement.NodeSelector
	if len(selector) == 0 || len(ev.Selector.MatchExpressions) == 0 {
		return false
	}
	_, prof := selectProfileNode(ev)
	if prof == nil {
		// Offline hasn't saturated yet — nothing to re-assert against.
		return false
	}

	anyChanged := false
	for _, ksvc := range ev.Selector.MatchExpressions[0].Values {
		k := ns + "/" + ksvc
		if owner, ok := claimed[k]; ok && owner != nimbus {
			continue // owned by another Nimbus this tick — silent skip
		}
		claimed[k] = nimbus

		bchanged := kubeapi.CreateStartupCPUBoost(ctx, ev, ksvc, prof.OptStarting)
		pchanged, perr := kubeapi.ApplyKsvcSpec(ctx, ns, ksvc, selector, prof.OptRunning)
		if perr != nil {
			logging.Failure(fmt.Sprintf("[online][error] event=offline_bootstrap_apply ns=%s nimbus=%s ksvc=%s reason=%v",
				ns, nimbus, ksvc, perr))
			continue
		}
		if bchanged || pchanged {
			anyChanged = true
			logging.Info(fmt.Sprintf("[online][bootstrap] event=offline_enforce ns=%s nimbus=%s ksvc=%s cold=%s warm=%s action=patch",
				ns, nimbus, ksvc, prof.OptStarting, prof.OptRunning))
		}
	}
	return anyChanged
}

// ReconcileOne is the polling self-healing fallback (OVERVIEW Option A): it
// re-asserts the §8.3 waterfall against live headroom for every managed ksvc,
// reading the shared BurstState but NOT feeding it (the /decide RPC is the
// cold-start event source). The /decide handler gives zero-staleness on the
// triggering pod; this loop catches fail-open + drift and owns the
// .status.online write (single writer).
//
// Per ksvc: choose the CPU tier (c_opt pool-wide → c_min pool-wide → Pending;
// no hostname pin — nodeSelector stays the pool selector offline wrote), apply
// the tier's cold value to the boost CR + warm value to the ksvc spec, deduct
// within-tick, and emit a status row. Pending rows carry degraded=true and are
// not patched.
//
// Level-triggered: nothing is written and nothing is logged when the outcome is
// unchanged from the previous tick.
//
// claimed maps "ns/ksvc" -> owning Nimbus name for the current tick so two
// Nimbuses can't fight over one ksvc. Returns (assignmentCount, changed);
// assignmentCount is -1 when the Nimbus is skipped (no selector / no profile /
// no Ready pool nodes).
func ReconcileOne(ctx context.Context, ev *nimbusevent.NimbusEvent, claimed map[string]string, last map[string]lastOutcome, bs *BurstState, pct int) (int, bool) {
	ns := ev.Metadata.Namespace
	nimbus := ev.Metadata.Name
	key := ns + "/" + nimbus

	if len(ev.Selector.MatchExpressions) == 0 {
		return -1, noteSkip(last, key, "no_selector", func() {
			logging.Warning(fmt.Sprintf("[online][warn] event=nimbus_skip_unsaturated ns=%s nimbus=%s action=skip reason=no_selector",
				ns, nimbus))
		})
	}

	// Per-Nimbus online opt-out (spec.online.enabled=false). Offline still
	// runs (binary search, profile, bootstrap apply, boost CR); the waterfall
	// + .status.online write + hostname pinning are skipped. BUT drift on
	// pool-selector / c_opt_warm CPU / max-scale=1 / boost CR is still
	// corrected every tick by enforceOfflineBootstrap — otherwise a third
	// party edit between Nimbus reconciles silently breaks the offline-only
	// baseline. /decide handles its own short-circuit (server.go).
	if !ev.Spec.OnlineEnabled() {
		if enforceOfflineBootstrap(ctx, ev, claimed) {
			// Drift was corrected this tick. Skip the "first-entry" noteSkip
			// log so a subsequent silent tick stays quiet (same skipReason
			// key keeps noteSkip's state machine consistent).
			last[key] = lastOutcome{skipReason: "online_disabled"}
			return -1, true
		}
		return -1, noteSkip(last, key, "online_disabled", func() {
			logging.Info(fmt.Sprintf("[online][nimbus] event=skip_offline_only ns=%s nimbus=%s action=skip reason=spec.online.enabled=false",
				ns, nimbus))
		})
	}

	_, profile := selectProfileNode(ev)
	if profile == nil {
		return -1, noteSkip(last, key, "no_complete_profile", func() {
			logging.Warning(fmt.Sprintf("[online][nimbus] event=nimbus_skip_unsaturated ns=%s nimbus=%s action=skip reason=no_complete_profile",
				ns, nimbus))
		})
	}

	snap, err := buildPoolSnapshot(ctx, ev.Spec.Placement.NodeSelector, ns, pct)
	if err != nil {
		return -1, noteSkip(last, key, "no_pool_nodes", func() {
			logging.Warning(fmt.Sprintf("[online][nimbus] event=nimbus_skip_unsaturated ns=%s nimbus=%s action=skip reason=no_pool_nodes detail=%v",
				ns, nimbus, err))
		})
	}
	mode, reserve, rate, deltaRate := bs.Read()
	poolLabel := poolLabelValue(ev.Spec.Placement.NodeSelector)
	warmPath := ev.Spec.DurationPolicy.WarmApiCondition.Path
	ksvcs := ev.Selector.MatchExpressions[0].Values
	now := metav1.NewTime(time.Now())

	// prevByKsvc indexes the previous tick's assignments so we can preserve
	// DecidedAt + per-row BurstRate when a ksvc's decision is unchanged.
	// DecidedAt records when the row FIRST took its current shape, not when
	// status was last written — only an actual decision change updates it.
	prevByKsvc := map[string]nimbusevent.OnlineAssignment{}
	if prev := last[key]; prev.online != nil {
		for _, a := range prev.online.Assignments {
			prevByKsvc[a.Ksvc] = a
		}
	}

	anyWrite := false
	// pending holds warnings (duplicate / missing / no-fit) flushed only if this
	// tick is a change — so a persistent condition doesn't re-warn every 2s.
	var pending []func()
	assignments := make([]nimbusevent.OnlineAssignment, 0, len(ksvcs))
	for _, ksvc := range ksvcs {
		ksvc := ksvc
		k := ns + "/" + ksvc
		if owner, ok := claimed[k]; ok {
			owner := owner
			pending = append(pending, func() {
				logging.Warning(fmt.Sprintf("[online][warn] event=duplicate_ksvc ns=%s ksvc=%s owner=%s duplicate=%s action=skip",
					ns, ksvc, owner, nimbus))
			})
			continue
		}
		claimed[k] = nimbus

		exists, ready := readKsvcReady(ctx, ns, ksvc)
		if !exists {
			pending = append(pending, func() {
				logging.Warning(fmt.Sprintf("[online][warn] event=ksvc_missing ns=%s nimbus=%s ksvc=%s action=skip reason=not_found",
					ns, nimbus, ksvc))
			})
			continue
		}

		// Waterfall: pick the CPU tier; deduct within-tick so later ksvcs see
		// reduced headroom. nodeSelector is untouched (pool selector stays).
		d := decideTier(profile, snap, mode, reserve)
		deductCold(snap, d)

		row := nimbusevent.OnlineAssignment{
			Ksvc:  ksvc,
			Ready: ready,
			URL:   kubeapi.BuildKsvcStatusURL(ns, ksvc, warmPath),
		}
		if !d.admitted {
			row.Degraded = true
			pending = append(pending, func() {
				logging.Warning(fmt.Sprintf("[online][warn] event=ksvc_pending ns=%s nimbus=%s ksvc=%s mode=%s action=no_admission reason=no_node_meets_c_opt_warm",
					ns, nimbus, ksvc, mode))
			})
		} else {
			// Tier 3 pins; Tiers 1/2 stay pool-wide. ApplyKsvcSpec replaces
			// nodeSelector wholesale, so a transition Tier3→Tier1/2 drops the
			// stale hostname pin automatically. The warm value is always
			// c_opt_warm (thesis: cold-start optimization only) — ApplyKsvcSpec
			// no-ops on identical CPU, so the call is cheap.
			selector := buildSelector(ev.Spec.Placement.NodeSelector, d)
			if d.node != "" {
				row.Node = d.node
			} else {
				row.Node = poolLabel
			}
			row.Tier = d.tier
			row.StartingCpu = d.cold
			row.RunningCpu = d.warm
			row.Degraded = d.degraded

			// Both helpers no-op when converged; log ksvc_decide only on a write.
			bchanged := kubeapi.CreateStartupCPUBoost(ctx, ev, ksvc, d.cold)
			pchanged, perr := kubeapi.ApplyKsvcSpec(ctx, ns, ksvc, selector, d.warm)
			if perr != nil {
				logging.Failure(fmt.Sprintf("[online][error] event=ksvc_apply ns=%s nimbus=%s ksvc=%s action=apply reason=%v",
					ns, nimbus, ksvc, perr))
			}
			if bchanged || pchanged {
				anyWrite = true
				logging.Info(fmt.Sprintf("[online][ksvc] event=ksvc_decide ns=%s nimbus=%s ksvc=%s tier=%s node=%s mode=%s cold=%s warm=%s degraded=%v action=patch",
					ns, nimbus, ksvc, d.tier, row.Node, mode, d.cold, d.warm, d.degraded))
			}
		}

		// DecidedAt / per-row BurstRate: preserve from the previous tick when
		// the decision tuple (Tier/Node/StartingCpu/RunningCpu/Degraded) is
		// unchanged; otherwise stamp the current time + current burst rate.
		// This way DecidedAt marks when the row first took its current shape.
		if prev, ok := prevByKsvc[ksvc]; ok &&
			prev.Tier == row.Tier && prev.Node == row.Node &&
			prev.StartingCpu == row.StartingCpu && prev.RunningCpu == row.RunningCpu &&
			prev.Degraded == row.Degraded {
			row.DecidedAt = prev.DecidedAt
			row.BurstRate = prev.BurstRate
		} else {
			row.DecidedAt = now
			row.BurstRate = rate
		}
		assignments = append(assignments, row)
	}

	desired := buildOnlineStatus(assignments, mode, rate, deltaRate)
	prev := last[key]
	statusChanged := prev.skipReason != "" || !reflect.DeepEqual(prev.online, desired)
	changed := anyWrite || statusChanged

	if changed {
		for _, f := range pending {
			f()
		}
	}

	if statusChanged {
		if err := kubeapi.WriteNimbusOnlineStatus(ctx, ns, nimbus, desired); err != nil {
			logging.Failure(fmt.Sprintf("[online][error] event=status_write ns=%s nimbus=%s action=write reason=%v",
				ns, nimbus, err))
			// Leave last unchanged so the next tick retries the write.
		} else {
			logging.Info(fmt.Sprintf("[online][status] event=status_write ns=%s nimbus=%s assignments=%d mode=%s action=write",
				ns, nimbus, len(assignments), mode))
			last[key] = lastOutcome{online: desired}
		}
	}

	if changed {
		logging.Info(fmt.Sprintf("[online][nimbus] event=nimbus_reconcile_complete ns=%s nimbus=%s assignments=%d",
			ns, nimbus, len(assignments)))
	}

	return len(assignments), changed
}

// readKsvcReady fetches the Knative service and reports whether it exists and
// whether its Ready condition is currently True. Any Get error is treated as
// "doesn't exist" for Phase 1 — the next tick retries. The ksvc is read via
// the shared dynamic client (online is read-only here; it mutates ksvcs only
// through the kubeapi helpers).
func readKsvcReady(ctx context.Context, ns, name string) (exists bool, ready bool) {
	obj, err := kubeconfig.DYNCLIENT.Resource(kubeconfig.KSVC_GVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false, false
	}
	conds, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return true, false
	}
	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if m["type"] == "Ready" && m["status"] == "True" {
			return true, true
		}
	}
	return true, false
}
