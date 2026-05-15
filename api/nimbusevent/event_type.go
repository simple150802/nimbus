package nimbusevent

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

	// High / Low are scratchpad bounds for the active binary-search loop.
	// They are reset at the top of each per-phase wrapper, so reusing the
	// same fields across nodes in a per-node loop is safe.
	High string `json:"-"`
	Low  string `json:"-"`

	// CandidateNodes is the list of cluster nodes the target ksvc is
	// eligible to run on, given its nodeSelector + nodeAffinity +
	// tolerations. Populated once when the event enters the worker, sorted
	// lexicographically. Empty until populated; nil after a discovery error.
	CandidateNodes []string `json:"-"`

	// PerNodeResults is the per-node binary-search outcome map, keyed by
	// node name. BinarySearch writes its converged CPU values directly
	// into the entry for the node it's currently measuring, so downstream
	// callers (status persistence, ksvc apply) read all results uniformly.
	// Populated incrementally as the per-node loop iterates.
	PerNodeResults map[string]*NodeResult `json:"-"`

	// AllSaturated is the outer skip flag — true iff every candidate node
	// has both StartingSaturated and RunningSaturated set. The worker
	// sets it after loading state from .status; when true, the entire
	// binary search is skipped (fast path).
	AllSaturated bool `json:"-"`

	// ExportRoot is the absolute filesystem path the controller writes
	// raw per-sample CSVs + meta.json + per-node result.json to during
	// this run's binary search. Set once by runMultiNodeSearch via
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
	StartingCpu       string        `json:"startingCpu,omitempty"`
	StartingRt        *RtStats      `json:"startingRt,omitempty"`
	RunningCpu        string        `json:"runningCpu,omitempty"`
	RunningRt         *RtStats      `json:"runningRt,omitempty"`
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

	ResourcePolicy ResourcePolicy    `json:"resourcePolicy"`
	DurationPolicy DurationPolicy    `json:"durationPolicy"`
	Measurement    MeasurementPolicy `json:"measurement,omitempty"`
	Export         *ExportSpec       `json:"export,omitempty"`
	PreMeasured    *PreMeasuredSpec  `json:"preMeasured,omitempty"`
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

type ContainerPolicy struct {
	ContainerName string        `json:"containerName"`
	ResourceRange ResourceRange `json:"resourceRange"`
}

type ResourceRange struct {
	Limits ResourceMinMax `json:"limits"`
}

type ResourceMinMax struct {
	Min string `json:"min"`
	Max string `json:"max"`
}

// --- Duration Policy Tree ---
type DurationPolicy struct {
	ApiCondition ApiCondition `json:"apiCondition"`
}

type ApiCondition struct {
	// Path is the URL path the controller GETs (e.g. "/status"). The full
	// URL is constructed per ksvc via kubeapi.BuildKsvcStatusURL — see
	// that helper for the format. Must start with '/' (enforced by the
	// CRD pattern).
	Path     string `json:"path"`
	Response string `json:"response"`
}
