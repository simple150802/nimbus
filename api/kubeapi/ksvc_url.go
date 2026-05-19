package kubeapi

import "fmt"

// BuildKsvcStatusURL constructs the cluster-internal probe URL for a
// Knative service. Format:
//
//	http://<ksvcName>.<namespace>.svc.cluster.local<path>
//
// path is taken verbatim from spec.durationPolicy.{cold,warm}ApiCondition.path
// (caller picks which); the CRD pattern guarantees it starts with '/'.
// Single source of truth so probe_cold / probe_warm / buildBoostCR all
// derive the URL the same way.
//
// HTTPS, non-default ports, and non-standard cluster DNS suffixes are
// out of scope — the thesis cluster uses the defaults. Add a scheme /
// domain arg here if a future cluster needs to override.
func BuildKsvcStatusURL(namespace, ksvcName, path string) string {
	return fmt.Sprintf("http://%s.%s.svc.cluster.local%s", ksvcName, namespace, path)
}
