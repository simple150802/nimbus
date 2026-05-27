package online

import (
	"context"
	"fmt"
	"reflect"

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

// ReconcileOne reconciles a single completed Nimbus snapshot under the Phase 1
// policy: assign the offline-converged c_opt tier to every listed ksvc — no
// headroom math, no hostname pinning, no burst awareness (those are Phase 3).
// For each existing ksvc it upserts the StartupCPUBoost CR, patches the running
// CPU, and emits one status.online assignment row; then it writes the whole row
// set to .status.online.
//
// Level-triggered: nothing is written and nothing is logged when the outcome is
// unchanged from the previous tick. last carries the prior outcome per Nimbus
// and is updated here. A change is "some ksvc patch/boost actually wrote" OR
// "the assignment set differs from last tick"; only then do the per-ksvc
// warnings, the status write, and the reconcile-complete line fire.
//
// claimed maps "ns/ksvc" -> owning Nimbus name for the current tick. A ksvc
// already claimed by an earlier Nimbus this tick is skipped with a
// duplicate_ksvc warning, so two completed Nimbuses can't fight over one ksvc.
//
// Returns (assignmentCount, changed); assignmentCount is -1 when the Nimbus is
// skipped because it has no selector or no complete offline profile yet.
func ReconcileOne(ctx context.Context, ev *nimbusevent.NimbusEvent, claimed map[string]string, last map[string]lastOutcome) (int, bool) {
	ns := ev.Metadata.Namespace
	nimbus := ev.Metadata.Name
	key := ns + "/" + nimbus

	if len(ev.Selector.MatchExpressions) == 0 {
		return -1, noteSkip(last, key, "no_selector", func() {
			logging.Warning(fmt.Sprintf("[online][warn] event=nimbus_skip_unsaturated ns=%s nimbus=%s action=skip reason=no_selector",
				ns, nimbus))
		})
	}

	node, profile := selectProfileNode(ev)
	if profile == nil {
		return -1, noteSkip(last, key, "no_complete_profile", func() {
			logging.Warning(fmt.Sprintf("[online][nimbus] event=nimbus_skip_unsaturated ns=%s nimbus=%s action=skip reason=no_complete_profile",
				ns, nimbus))
		})
	}

	startingCpu := profile.StartingCpu
	runningCpu := profile.RunningCpu
	warmPath := ev.Spec.DurationPolicy.WarmApiCondition.Path
	ksvcs := ev.Selector.MatchExpressions[0].Values

	anyWrite := false
	// pending holds warnings (duplicate ksvc / missing ksvc) that are flushed
	// only if this tick is a change — so a persistently-missing ksvc doesn't
	// re-warn every 2s.
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

		// c_opt apply: per-ksvc StartupCPUBoost (cold value) + running CPU.
		// Both helpers no-op when already converged; we log ksvc_decide only
		// when one of them actually wrote.
		bchanged := kubeapi.CreateStartupCPUBoost(ctx, ev, ksvc, startingCpu)
		pchanged, err := kubeapi.PatchResourceLimits(ctx, ns, ksvc, runningCpu)
		if err != nil {
			logging.Failure(fmt.Sprintf("[online][error] event=ksvc_patch_cpu ns=%s nimbus=%s ksvc=%s action=patch reason=%v",
				ns, nimbus, ksvc, err))
			// Record the row anyway; the next tick re-attempts the patch.
		}
		if bchanged || pchanged {
			anyWrite = true
			logging.Info(fmt.Sprintf("[online][ksvc] event=ksvc_decide ns=%s nimbus=%s ksvc=%s node=%s tier=%s starting=%s running=%s action=patch",
				ns, nimbus, ksvc, node, nimbusevent.TierCOpt, startingCpu, runningCpu))
		}

		assignments = append(assignments, nimbusevent.OnlineAssignment{
			Ksvc:        ksvc,
			Node:        node,
			Tier:        nimbusevent.TierCOpt,
			StartingCpu: startingCpu,
			RunningCpu:  runningCpu,
			Ready:       ready,
			URL:         kubeapi.BuildKsvcStatusURL(ns, ksvc, warmPath),
		})
	}

	desired := buildOnlineStatus(assignments)
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
			logging.Info(fmt.Sprintf("[online][status] event=status_write ns=%s nimbus=%s assignments=%d action=write",
				ns, nimbus, len(assignments)))
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
