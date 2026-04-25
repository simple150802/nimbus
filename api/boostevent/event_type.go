package boostevent

// ---------------------------------------------------------
// 1. The Main Event Wrapper
// ---------------------------------------------------------
type BoostEvent struct {
	Metadata BoostMetadata `json:"metadata"`
	Selector BoostSelector `json:"selector"`
	Spec     BoostSpec     `json:"spec"`

	// Status mirrors the CRD's .status subresource. When both StartingCpu and
	// RunningCpu are non-empty, the binary search has already been completed
	// for this Recon and the worker skips re-running it.
	Status BoostStatus `json:"status"`

	Next *BoostEvent `json:"-"`

	High string `json:"-"`
	Low  string `json:"-"`

	StartingSaturated bool   `json:"-"`
	StartingCPU       string `json:"-"`

	RunningSaturated bool   `json:"-"`
	RunningCPU       string `json:"-"`
}

// BoostStatus reflects the Recon CRD's .status subresource. Field names must
// match the JSON keys declared in config/crd.yaml exactly.
type BoostStatus struct {
	StartingCpu string `json:"startingCpu,omitempty"`
	RunningCpu  string `json:"runningCpu,omitempty"`
}

// ---------------------------------------------------------
// 2. Metadata & Selector
// ---------------------------------------------------------
type BoostMetadata struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type BoostSelector struct {
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
type BoostSpec struct {
	ResourcePolicy ResourcePolicy    `json:"resourcePolicy"`
	DurationPolicy DurationPolicy    `json:"durationPolicy"`
	Measurement    MeasurementPolicy `json:"measurement,omitempty"`
}

// MeasurementPolicy controls how many samples the controller averages per
// probe. When the Recon omits this field the CRD defaults (1 cold sample,
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
