package nimbusevent

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ---------------------------------------------------------
// 1. The Main Event Wrapper
// ---------------------------------------------------------
type NimbusEvent struct {
	Metadata NimbusMetadata `json:"metadata"`
	Selector NimbusSelector `json:"selector"`
	Spec     NimbusSpec     `json:"spec"`

	// Status mirrors the CRD's .status subresource. The Nimbus is
	// considered "completed" when status.perNode covers every candidate
	// node with non-empty StartingCpu and RunningCpu values.
	Status NimbusStatus `json:"status"`

	Next *NimbusEvent `json:"-"`

	// CandidateNodes is the offline measurement target set. In the
	// node-pool-only POC it contains exactly one representative node selected
	// from spec.placement.nodeSelector. Empty until populated; nil after a
	// discovery error.
	CandidateNodes []string `json:"-"`

	// PerNodeResults is the binary-search outcome map, keyed by measured node
	// name. In the node-pool-only POC this normally has one representative
	// entry, interpreted as the profile for the whole Nimbus node pool.
	PerNodeResults map[string]*NodeResult `json:"-"`

	// AllSaturated is the outer skip flag — true iff the representative
	// profile has both StartingSaturated and RunningSaturated set. The worker
	// sets it after loading state from .status; when true, the binary search
	// is skipped (fast path).
	AllSaturated bool `json:"-"`

	// ExportRoot is the absolute filesystem path the controller writes
	// raw per-sample CSVs + meta.json + per-node result.json to during
	// this run's binary search. Set once by runNodePoolSearch via
	// internal/export.InitRunDir; empty string means export is disabled
	// (spec.export unset or directory uncreatable). Probe helpers read
	// this field to decide whether to write per-probe CSVs.
	ExportRoot string `json:"-"`
}

// NodeResult is the converged CPU pair for one candidate node. CPU fields
// are empty until the corresponding phase finishes on that node. The two
// Saturated booleans are the inner skip markers — they mirror the old
// flat StartingSaturated / RunningSaturated semantic but at per-node
// granularity. Runtime-only (json:"-"); recomputed from CPU emptiness on
// load so they don't need to round-trip through .status.
//
// ColdRtSamples / WarmRtSamples capture every (cpu, stats) probe point the
// binary search visited, in ascending-cpu order. The online stage's
// algorithms consume these as the per-pod RT_i(x) curve via piecewise-
// linear interpolation. One SamplePoint per probe call (cpu = the CPU
// just probed; rt = stats across that probe's coldSamples/warmSamples,
// computed at end-of-loop). Sorted at end-of-phase by ascending cpu.
//
// StartingRt / RunningRt are the *saturated* RT stats — i.e., the per-
// percentile RT measured at the converged CPU value for each phase. They
// are nil until the corresponding phase saturates, then populated by the
// binary-search setResult callback by looking up the latest probe at the
// converged CPU. Useful for SLO comparisons (e.g. "at the recommended
// CPU, what's the p95 latency?") without re-deriving from the sample
// list.
type NodeResult struct {
	// StartingCpu / RunningCpu are c_opt — the latency-plateau edge — for
	// the cold and warm phases respectively, computed by the offline binary
	// search per node.
	StartingCpu string   `json:"startingCpu,omitempty"`
	StartingRt  *RtStats `json:"startingRt,omitempty"`
	RunningCpu  string   `json:"runningCpu,omitempty"`
	RunningRt   *RtStats `json:"runningRt,omitempty"`

	// CMinStarting / CMinRunning are c_min — the smallest probed CPU at
	// which the gate metric meets spec.acceptableResponseTime — for the
	// cold and warm phases. Derived by DeriveMin from the corresponding
	// sample list at end-of-phase. Empty string means "no probed sample
	// met the SLO budget" (or spec.acceptableResponseTime.<phase> was
	// absent for this phase). Consumed by the online stage's waterfall.
	CMinStarting string `json:"cMinStarting,omitempty"`
	CMinRunning  string `json:"cMinRunning,omitempty"`

	ColdRtSamples     []SamplePoint `json:"coldRtSamples,omitempty"`
	WarmRtSamples     []SamplePoint `json:"warmRtSamples,omitempty"`
	StartingSaturated bool          `json:"-"`
	RunningSaturated  bool          `json:"-"`
}

// RtStats summarises a probe's measured response times across its
// individual samples. AvgMillis is the arithmetic mean (drives the
// binary-search convergence math); P90Millis / P95Millis are nearest-rank
// percentiles for downstream SLO analysis.
type RtStats struct {
	AvgMillis int64 `json:"avgMillis"`
	P90Millis int64 `json:"p90Millis"`
	P95Millis int64 `json:"p95Millis"`
}

// SamplePoint is one (cpu, rt-stats) measurement from a single binary-
// search probe. Cpu is a Kubernetes-quantity string ("706m", "1500m");
// RtMillis is the mean RT in milliseconds (kept for backward compatibility
// with existing consumers); RtP90Millis / RtP95Millis carry the same-
// probe percentiles. All values int64 so the wire form reads cleanly via
// `kubectl get -o yaml`.
type SamplePoint struct {
	Cpu         string `json:"cpu"`
	RtMillis    int64  `json:"rtMillis"`
	RtP90Millis int64  `json:"rtP90Millis,omitempty"`
	RtP95Millis int64  `json:"rtP95Millis,omitempty"`
}

// NimbusStatus reflects the Nimbus CRD's .status subresource. Field names must
// match the JSON keys declared in config/crd.yaml exactly.
type NimbusStatus struct {
	// RunningCpu is the cluster-wide steady-state CPU limit applied to
	// the ksvc spec. Derived as max over PerNode[*].RunningCpu so the
	// slowest node still meets the running-phase target. Operators read
	// this via `kubectl get nimbus -o yaml` to see which value the
	// controller chose; it is rewritten whenever PerNode changes.
	RunningCpu string `json:"runningCpu,omitempty"`

	// StartingCpu is the cluster-wide cold-phase CPU limit applied via
	// StartupCPUBoost. Derived as max over PerNode[*].StartingCpu so the
	// slowest node still cold-starts within the configured RT budget.
	StartingCpu string `json:"startingCpu,omitempty"`

	// PerNode keys are node names. A Nimbus is considered "completed"
	// when every candidate node has a non-empty StartingCpu and RunningCpu
	// entry here. The aggregate RunningCpu / StartingCpu fields above are
	// derived from this map and are purely observational — the runtime
	// path computes max on the fly when applying values.
	PerNode map[string]NodeResult `json:"perNode,omitempty"`

	// Online is the online-stage reconciler's output. Populated only after
	// offline saturation; absent (nil) while the offline binary search is
	// still running or has nothing to consume. One assignment row per ksvc
	// listed in spec.selector.matchExpressions[0].values.
	Online *OnlineStatus `json:"online,omitempty"`

	// Applied records what the offline-phase apply loop attempted to write
	// onto each ksvc in spec.selector.matchExpressions[0].values during the
	// most recent reconcile tick. Keyed by ksvc name. Rewritten wholesale
	// on every successful tick — a non-empty ApplyError on an entry means
	// the apiserver rejected one of the patches and the ksvc may not match
	// what NodeSelector / RunningCpu claim. Consumed by operators (and the
	// future online reconciler) to verify the invariant that every ksvc's
	// nodeSelector equals spec.placement.nodeSelector after offline.
	Applied map[string]KsvcApplyState `json:"applied,omitempty"`
}

// KsvcApplyState is one row of NimbusStatus.Applied — the offline-phase
// apply loop's per-ksvc outcome for one reconcile tick. NodeSelector is
// what NIMBUS wrote (not what the ksvc currently has — those can drift
// if a third party patches the ksvc after the tick); ApplyError is the
// Go error.Error() of the first failure inside the apply loop, with
// later patches in the same tick skipped to avoid a partial mutation.
type KsvcApplyState struct {
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	StartingCpu  string            `json:"startingCpu,omitempty"`
	RunningCpu   string            `json:"runningCpu,omitempty"`
	ApplyError   string            `json:"applyError,omitempty"`
}

// Tier identifies which performance band a ksvc was assigned to. Mirrors
// the CRD enum on status.online.assignments[].tier. Per future_plan.md
// §6, only two tiers exist — there is no "c_floor" fallback; when
// neither c_opt nor c_min fits, the online /decide call returns Pending
// (KPA aborts the scale-up) rather than admitting at a sub-c_min CPU.
const (
	// TierCOpt is the offline-converged knee/optimal CPU per node — the
	// highest tier; consumes the most headroom but gives the best RT.
	TierCOpt = "c_opt"
	// TierCMin is the smallest sampled CPU still meeting
	// spec.acceptableResponseTime. Derived from the offline sample list
	// via DeriveMin. NIMBUS's fallback tier when c_opt doesn't fit.
	TierCMin = "c_min"
	// TierBestFit is the degraded fallback: when neither c_opt nor c_min
	// fits pool-wide, NIMBUS pins the ksvc to the node with the most free
	// CPU and uses all of that node's available room as the cold/boost CPU.
	// The warm side stays at c_opt_warm (thesis scope: cold-start
	// optimization only). degraded=true on these assignment rows.
	TierBestFit = "best_fit"
)

// OnlineStatus is the .status.online subtree — the online reconciler's
// output. Written once per reconcile after offline is saturated. Old
// rows are replaced wholesale, not merged.
type OnlineStatus struct {
	// ActiveAssignments equals len(Assignments). Exposed as a top-level
	// field so `kubectl get -o wide` and quick health checks can read it
	// without parsing the array.
	ActiveAssignments int `json:"activeAssignments"`

	// BurstMode is the cluster-wide burst-detector state ("NORMAL" | "BURST")
	// that drove the most recent reconcile. Recorded for experiment-CSV
	// correlation; the detector itself is process-global, not per-Nimbus.
	BurstMode string `json:"burstMode,omitempty"`

	// BurstRate is the smoothed cold-start arrival rate (events/sec) at the
	// time of the most recent status write. Snapshot, not continuous —
	// updates only when something else in the status changes (level-trigger).
	// Useful for joining a thesis-experiment latency CSV row to "what was the
	// detector signal when this decision was made".
	BurstRate float64 `json:"burstRate,omitempty"`

	// BurstDeltaRate is the smoothed acceleration (events/sec²) at the same
	// instant as BurstRate. Justifies §5.6 early-flip behaviour with data.
	BurstDeltaRate float64 `json:"burstDeltaRate,omitempty"`

	// Assignments is one row per managed ksvc, in the order they appear
	// in spec.selector.matchExpressions[0].values. A ksvc is omitted only
	// when it does not exist in the cluster; otherwise a row is produced
	// even when the only feasible assignment is c_floor with degraded=true.
	Assignments []OnlineAssignment `json:"assignments,omitempty"`
}

// OnlineAssignment is the policy assigned to one ksvc by the waterfall
// scheduler. The Ksvc + Node + Tier triple is the primary key for joining
// experiment-script CSV rows back to the controller's decision.
type OnlineAssignment struct {
	// Ksvc is the Knative service name this row describes. Always one of
	// spec.selector.matchExpressions[0].values.
	Ksvc string `json:"ksvc"`

	// Node is the Kubernetes node chosen for this ksvc. Written to the
	// ksvc as spec.template.spec.nodeSelector["kubernetes.io/hostname"].
	Node string `json:"node"`

	// Tier is one of TierCOpt / TierCMin. See those constants for the
	// per-tier source rules. There is no c_floor tier — when neither
	// c_opt nor c_min fits, /decide returns Pending (KPA aborts the
	// scale-up) rather than admitting at a sub-c_min CPU.
	Tier string `json:"tier"`

	// StartingCpu is the cold-phase CPU written into the per-ksvc
	// StartupCPUBoost CR. k8s-quantity string ("931m", "1500m").
	StartingCpu string `json:"startingCpu"`

	// RunningCpu is the steady-state CPU patched onto the ksvc itself.
	// Always <= StartingCpu.
	RunningCpu string `json:"runningCpu"`

	// Degraded is set when /decide refused to assign at any tier
	// (neither c_opt nor c_min fit anywhere with available headroom).
	// The online status row still gets emitted for traceability — Tier
	// will be "" and the experiment script can correlate degraded
	// assignments with SLO breaches in its CSV output.
	Degraded bool `json:"degraded,omitempty"`

	// Ready mirrors the target ksvc's Ready condition at the last
	// reconcile. ready=false means the patch landed but the ksvc didn't
	// come up healthy under the assigned CPU.
	Ready bool `json:"ready,omitempty"`

	// URL is the cluster-internal URL for this ksvc, built via
	// kubeapi.BuildKsvcStatusURL. Cached on status so the experiment
	// script doesn't need a separate kubectl-get-ksvc per row.
	URL string `json:"url,omitempty"`

	// DecidedAt is the wall-clock time the placement decision was made.
	// PRESERVED across ticks when the decision (Tier/Node/StartingCpu/
	// RunningCpu/Degraded) is unchanged — so it records when the row first
	// took its current shape, not when status was last written. Drives the
	// time-series join in the experiment harness.
	DecidedAt metav1.Time `json:"decidedAt,omitempty"`

	// BurstRate is the smoothed cold-start arrival rate (events/sec) at
	// DecidedAt — i.e. the detector signal that drove THIS row's decision.
	// Preserved alongside DecidedAt across unchanged ticks.
	BurstRate float64 `json:"burstRate,omitempty"`
}

// ---------------------------------------------------------
// 2. Metadata & Selector
// ---------------------------------------------------------
type NimbusMetadata struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type NimbusSelector struct {
	MatchExpressions []MatchExpression `json:"matchExpressions"`
}

type MatchExpression struct {
	Key      string   `json:"key"`
	Operator string   `json:"operator"`
	Values   []string `json:"values"`
}

// ---------------------------------------------------------
// 3. The Core Spec
// ---------------------------------------------------------
type NimbusSpec struct {
	// Metric is the RT percentile the binary search gates on — one of
	// "avg", "p90", "p95". Empty string is treated as the CRD default
	// ("p95"). The spec field is the only place this lives; the run-
	// time gate selector in algorithm.metricGate reads it directly.
	// Echoed into meta.json via the spec snapshot so the online stage
	// knows which metric drove the saturated CPU it's about to consume.
	Metric string `json:"metric,omitempty"`

	// AcceptableResponseTime carries the online-stage's pack-target
	// budgets. The offline binary search ignores this field; the
	// online tier waterfall walks the per-node sample list and derives
	// c_min for each node as the smallest probed CPU where the gate
	// metric is at or below the budget. Absent = no middle tier; the
	// waterfall degenerates to c_opt -> c_floor.
	AcceptableResponseTime *AcceptableResponseTimeSpec `json:"acceptableResponseTime,omitempty"`

	// Placement declares the Nimbus-owned node pool. Offline profiling resolves
	// this selector to Ready nodes and measures one representative node; online
	// applies the same selector to every controlled ksvc.
	Placement PlacementSpec `json:"placement,omitempty"`

	ResourcePolicy ResourcePolicy    `json:"resourcePolicy"`
	DurationPolicy DurationPolicy    `json:"durationPolicy"`
	Measurement    MeasurementPolicy `json:"measurement,omitempty"`
	Export         *ExportSpec       `json:"export,omitempty"`
	PreMeasured    *PreMeasuredSpec  `json:"preMeasured,omitempty"`

	// Online configures the online stage's behaviour for this Nimbus.
	// Absent or `online.enabled: true` keeps the default (online active).
	// `online.enabled: false` opts the Nimbus out — see OnlineSpec for the
	// exact effect on each code path.
	Online *OnlineSpec `json:"online,omitempty"`
}

// OnlineSpec is the per-Nimbus online-stage configuration. Today it only
// carries the on/off switch; the nested-struct shape leaves room to add
// per-Nimbus online tunables later (e.g. burst overrides) without another
// CRD migration.
type OnlineSpec struct {
	// Enabled controls whether the online stage's ADAPTIVE behaviour acts on
	// this Nimbus. Pointer-bool so we can distinguish unset (treated as true)
	// from explicit false. When false:
	//   - The polling reconciler skips the WATERFALL (no c_min downgrade, no
	//     best_fit hostname pinning) and the .status.online write — but DOES
	//     re-assert offline's bootstrap state every tick (pool selector,
	//     c_opt_warm CPU, max-scale=1, boost CR at c_opt_cold) via
	//     enforceOfflineBootstrap. So drift gets corrected without waiting
	//     for the next Nimbus CR edit.
	//   - /decide returns "passthrough" for managed ksvcs (KPA proceeds with
	//     the existing ksvc spec — already pool-wide at c_opt_warm from
	//     offline + the polling reconciler's continuous re-assertion).
	//   - The burst detector STILL observes the cold-start (cluster-wide
	//     rate must remain accurate so other Nimbuses' waterfalls react
	//     correctly).
	//
	// Offline is untouched: the binary search, profile persistence, and
	// per-ksvc StartupCPUBoost CR at c_opt_cold all happen regardless of
	// this flag. Offline-only mode is fully functional for cold-start boost;
	// what disappears is only the adaptive waterfall (c_min downgrade under
	// pressure, best_fit pin under saturation). Drift correction is preserved.
	Enabled *bool `json:"enabled,omitempty"`
}

// OnlineEnabled returns true when the online stage should act on this Nimbus.
// Default is true (field unset or Online block absent) so existing manifests
// keep current behaviour with no CRD migration.
func (s NimbusSpec) OnlineEnabled() bool {
	if s.Online == nil || s.Online.Enabled == nil {
		return true
	}
	return *s.Online.Enabled
}

// PlacementSpec is the Nimbus-owned scheduling scope. For the thesis POC,
// nodeSelector is the only supported placement input: every controlled ksvc
// belongs to this pool, and offline uses it to choose one representative node.
type PlacementSpec struct {
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

// AcceptableResponseTimeSpec carries per-phase RT budgets in
// milliseconds. Cold is required when the parent object is present;
// Warm is optional — absent means the online stage skips c_min
// derivation for the warm phase entirely. Field types are int64 to
// match RtStats.{Avg,P90,P95}Millis so the budget can be compared
// directly against persisted sample values without conversion.
type AcceptableResponseTimeSpec struct {
	Cold int64 `json:"cold"`
	Warm int64 `json:"warm,omitempty"`
}

// ExportSpec configures filesystem export of raw per-probe samples.
// When the Nimbus omits spec.export, no export happens. When set, the
// controller creates <Dir>/<timestamp>/ at search start and writes
// meta.json, per-node result.json, and per-probe <cpu>.csv files there.
type ExportSpec struct {
	Dir string `json:"dir"`
}

// PreMeasuredSpec configures one-time loading of a previously-exported
// run as the starting state of a fresh Nimbus. When LoadFromDir is set,
// the controller reads <dir>/<node>/result.json for each node referenced
// in the candidate set and pre-populates PerNodeResults; saturated nodes
// skip the binary search. Status overlays preMeasured — values already
// in .status.perNode win over the loaded data.
type PreMeasuredSpec struct {
	LoadFromDir string `json:"loadFromDir"`
}

// MeasurementPolicy controls how many samples the controller averages per
// probe. When the Nimbus omits this field the CRD defaults (1 cold sample,
// 10 warm samples) apply; values of 0 or less fall back to those defaults
// defensively so the controller never divides by zero.
type MeasurementPolicy struct {
	ColdSamples int `json:"coldSamples,omitempty"`
	WarmSamples int `json:"warmSamples,omitempty"`
}

// --- Resource Policy Tree ---
type ResourcePolicy struct {
	ContainerPolicies []ContainerPolicy `json:"containerPolicies"`
}

// ContainerPolicy is the per-container CPU budget input. The binary
// search anchors at CpuBudget (the economic ceiling) and bisects
// downward to discover c_opt and c_min — the operator does not supply
// a lower bound; the algorithm's resolution floor + a hardcoded safety
// floor (MinProbeCpuMilli) handle that.
type ContainerPolicy struct {
	ContainerName string `json:"containerName"`
	// CpuBudget is the maximum CPU per ksvc NIMBUS will ever assign for
	// this container. Kubernetes-quantity string (e.g. "2", "2000m").
	CpuBudget string `json:"cpuBudget"`
}

// --- Duration Policy Tree ---
type DurationPolicy struct {
	// ColdApiCondition is the gate for the offline cold phase. The
	// upstream kube-startup-cpu-boost webhook also polls this URL to
	// decide when each boost ends. Renamed from the historical
	// `apiCondition` field (single shared gate) when the warm phase got
	// its own dedicated condition.
	ColdApiCondition ApiCondition `json:"coldApiCondition"`

	// WarmApiCondition is the gate for the offline warm phase. The
	// previous design reused ColdApiCondition (a /status flag-read),
	// which is CPU-independent and made warm-phase binary search
	// converge on noise instead of signal. The warm phase now hits a
	// real workload endpoint whose RT scales with CPU.
	WarmApiCondition WarmApiCondition `json:"warmApiCondition"`
}

// ApiCondition is the historical cold-phase gate shape: GET <path> and
// look for Response as a substring of the body. Preserved verbatim so
// the cold-phase probe and the StartupCPUBoost CR (which polls the same
// URL with the same body-substring rule) keep their existing semantics.
type ApiCondition struct {
	// Path is the URL path the controller GETs (e.g. "/status"). The full
	// URL is constructed per ksvc via kubeapi.BuildKsvcStatusURL — see
	// that helper for the format. Must start with '/' (enforced by the
	// CRD pattern).
	Path     string `json:"path"`
	Response string `json:"response"`
}

// WarmApiCondition is the warm-phase gate. Shape diverges from
// ApiCondition because a workload endpoint's response body varies per
// request — gating on a substring is fragile, so we gate primarily on
// HTTP status code (StatusCode) with an optional defensive body-substring
// check (BodyContains). Pass rule: actual code == StatusCode AND
// (BodyContains == "" OR body contains it).
type WarmApiCondition struct {
	// Path is the URL path the controller GETs (e.g. "/detect/local").
	// Must start with '/' (enforced by the CRD pattern).
	Path string `json:"path"`

	// StatusCode is the single HTTP status code that means "success".
	// For an inference endpoint this is typically 200.
	StatusCode int `json:"statusCode"`

	// BodyContains is an optional defensive check. When non-empty, the
	// response body must contain it. Useful when the upstream
	// occasionally returns 200 with an error payload.
	BodyContains string `json:"bodyContains,omitempty"`
}
