package boostevent

// ---------------------------------------------------------
// 1. The Main Event Wrapper
// ---------------------------------------------------------
type BoostEvent struct {
	Metadata BoostMetadata `json:"metadata"`
	Selector BoostSelector `json:"selector"`
	Spec     BoostSpec     `json:"spec"`

	Next *BoostEvent `json:"-"`

	High      string `json:"-"`
	Low       string `json:"-"`
	Saturated bool   `json:"-"`
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
	ResourcePolicy ResourcePolicy `json:"resourcePolicy"`
	DurationPolicy DurationPolicy `json:"durationPolicy"`
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
