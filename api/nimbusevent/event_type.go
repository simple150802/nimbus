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
}

// NodeResult is the converged CPU pair for one candidate node. CPU fields
// are empty until the corresponding phase finishes on that node. The two
// Saturated booleans are the inner skip markers — they mirror the old
// flat StartingSaturated / RunningSaturated semantic but at per-node
// granularity. Runtime-only (json:"-"); recomputed from CPU emptiness on
// load so they don't need to round-trip through .status.
//
// ColdRtSamples / WarmRtSamples capture every (cpu, rt) probe point the
// binary search visited, in ascending-cpu order. The online stage's
// algorithms (algorithm.md §2.2.3) consume these as the per-pod RT_i(x)
// curve via piecewise-linear interpolation. One SamplePoint per probe
// call (cpu = the CPU just probed, rt = the average across that probe's
// coldSamples / warmSamples). Sorted at end-of-phase by ascending cpu.
type NodeResult struct {
	StartingCpu       string        `json:"startingCpu,omitempty"`
	RunningCpu        string        `json:"runningCpu,omitempty"`
	ColdRtSamples     []SamplePoint `json:"coldRtSamples,omitempty"`
	WarmRtSamples     []SamplePoint `json:"warmRtSamples,omitempty"`
	StartingSaturated bool          `json:"-"`
	RunningSaturated  bool          `json:"-"`
}

// SamplePoint is one (cpu, rt) measurement from a single binary-search
// probe. Cpu is a Kubernetes-quantity string ("706m", "1500m"); RtMillis
// is the response time in milliseconds (int64 instead of time.Duration
// so the wire form reads cleanly via `kubectl get -o yaml`).
type SamplePoint struct {
	Cpu      string `json:"cpu"`
	RtMillis int64  `json:"rtMillis"`
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
	ResourcePolicy ResourcePolicy    `json:"resourcePolicy"`
	DurationPolicy DurationPolicy    `json:"durationPolicy"`
	Measurement    MeasurementPolicy `json:"measurement,omitempty"`
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
	Url      string `json:"url"`
	Response string `json:"response"`
}
