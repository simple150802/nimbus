package online

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"nimbus/api/kubeapi"
	"nimbus/api/logging"
	"nimbus/api/nimbusevent"
	"nimbus/internal/watcher"
)

const defaultDecideAddr = ":8080"

// decideRequest is the body the (forked) KPA POSTs on a 0->1 cold-start.
type decideRequest struct {
	Namespace string `json:"namespace"`
	Ksvc      string `json:"ksvc"`
}

// decideResponse is what /decide returns to KPA.
//
//	admit       → KPA scales up; the ksvc spec + boost CR were just patched to
//	              (cpu, boostCpu); nodeSelector is the pool selector (kube-
//	              scheduler picks the node).
//	pending     → neither tier fits the budget anywhere; KPA aborts the scale-up.
//	passthrough → this ksvc isn't NIMBUS-managed (or not profiled yet); KPA
//	              proceeds with the existing spec, NIMBUS unchanged.
type decideResponse struct {
	Decision     string            `json:"decision"` // admit | pending | passthrough
	Tier         string            `json:"tier,omitempty"`
	Mode         string            `json:"mode,omitempty"`
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	Cpu          string            `json:"cpu,omitempty"`      // warm — ksvc spec
	BoostCpu     string            `json:"boostCpu,omitempty"` // cold — boost CR
}

// StartDecideServer runs the synchronous /decide endpoint until ctx is
// cancelled (OVERVIEW §8.3). It is the primary online trigger: the forked KPA
// calls it at the moment of a 0->1 cold-start. The polling reconciler
// (StartController) is the self-healing fallback. Launch as
// `go online.StartDecideServer(ctx, nw, bs)`.
func StartDecideServer(ctx context.Context, nw *watcher.NimbusWatcher, bs *BurstState) {
	addr := defaultDecideAddr
	if v := os.Getenv("NIMBUS_DECIDE_ADDR"); v != "" {
		addr = v
	}
	pct := budgetPct()

	mux := http.NewServeMux()
	mux.HandleFunc("/decide", func(w http.ResponseWriter, r *http.Request) {
		handleDecide(ctx, nw, bs, pct, w, r)
	})
	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	logging.Info(fmt.Sprintf("[online][decide] event=server_start addr=%s budget_pct=%d action=start", addr, pct))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logging.Failure("[online][decide] event=server_error reason:", err)
	}
}

// handleDecide runs the §8.3 waterfall for one cold-start and patches the ksvc
// spec + boost CR before returning. Status.online is owned by the polling
// reconciler (single writer), so /decide does not write it here.
func handleDecide(ctx context.Context, nw *watcher.NimbusWatcher, bs *BurstState, pct int, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req decideRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Namespace == "" || req.Ksvc == "" {
		http.Error(w, "body must be {namespace, ksvc}", http.StatusBadRequest)
		return
	}

	ev := findManagingNimbus(nw, req.Namespace, req.Ksvc)
	if ev == nil {
		writeDecide(w, decideResponse{Decision: "passthrough"})
		return
	}
	_, prof := selectProfileNode(ev)
	if prof == nil {
		// Offline hasn't saturated a profile yet — let KPA proceed unchanged.
		writeDecide(w, decideResponse{Decision: "passthrough"})
		return
	}

	// Feed the burst detector: this RPC IS the cold-start event.
	bs.OnColdStartEvent(time.Now())
	mode, reserve, rate := bs.Read()

	snap, err := buildPoolSnapshot(ctx, ev.Spec.Placement.NodeSelector, req.Namespace, pct)
	if err != nil {
		logging.Warning(fmt.Sprintf("[online][decide] event=snapshot_failed ns=%s ksvc=%s action=passthrough reason=%v",
			req.Namespace, req.Ksvc, err))
		writeDecide(w, decideResponse{Decision: "passthrough"})
		return
	}

	d := decideTier(prof, snap, mode, reserve)
	if !d.admitted {
		logging.Warning(fmt.Sprintf("[online][decide] event=pending ns=%s ksvc=%s mode=%s rate=%.2f reason=no_tier_fits",
			req.Namespace, req.Ksvc, mode, rate))
		writeDecide(w, decideResponse{Decision: "pending", Mode: mode.String()})
		return
	}

	// Patch before returning so kube-scheduler reads NIMBUS's intent.
	// Tier 3 pins via the hostname-augmented selector; Tier 1/2 stay pool-wide.
	// The warm side (ksvc.spec) is always c_opt_warm (thesis scope: cold-only
	// optimization) — ApplyKsvcSpec no-ops on identical CPU.
	selector := buildSelector(ev.Spec.Placement.NodeSelector, d)
	kubeapi.CreateStartupCPUBoost(ctx, ev, req.Ksvc, d.cold)
	if _, perr := kubeapi.ApplyKsvcSpec(ctx, req.Namespace, req.Ksvc, selector, d.warm); perr != nil {
		logging.Failure(fmt.Sprintf("[online][decide] event=apply_failed ns=%s ksvc=%s reason=%v", req.Namespace, req.Ksvc, perr))
	}
	logging.Info(fmt.Sprintf("[online][decide] event=admit ns=%s ksvc=%s tier=%s node=%s mode=%s warm=%s cold=%s degraded=%v",
		req.Namespace, req.Ksvc, d.tier, d.node, mode, d.warm, d.cold, d.degraded))

	writeDecide(w, decideResponse{
		Decision:     "admit",
		Tier:         d.tier,
		Mode:         mode.String(),
		NodeSelector: selector,
		Cpu:          d.warm,
		BoostCpu:     d.cold,
	})
}

// findManagingNimbus returns the completed Nimbus snapshot whose selector lists
// ksvc in the given namespace, or nil if none manages it.
func findManagingNimbus(nw *watcher.NimbusWatcher, ns, ksvc string) *nimbusevent.NimbusEvent {
	for _, ev := range nw.ListCompleted() {
		if ev.Metadata.Namespace != ns || len(ev.Selector.MatchExpressions) == 0 {
			continue
		}
		for _, v := range ev.Selector.MatchExpressions[0].Values {
			if v == ksvc {
				return ev
			}
		}
	}
	return nil
}

func writeDecide(w http.ResponseWriter, resp decideResponse) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
