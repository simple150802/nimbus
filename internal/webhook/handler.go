package webhook

// import (
// 	"encoding/json"
// 	"fmt"
// 	"net/http"

// 	v1 "k8s.io/api/admission/v1"
// )

// func HandleMutation(w http.ResponseWriter, r *http.Request) {
// 	// 1. Decode the AdmissionReview from K8s API
// 	var admissionReview v1.AdmissionReview
// 	json.NewDecoder(r.Body).Decode(&admissionReview)

// 	// 2. Parse the Knative Service from the request
// 	raw := admissionReview.Request.Object.Raw
// 	var svc knativev1.Service
// 	json.Unmarshal(raw, &svc)

// 	// 3. FETCH: Use your kubeapi to find the BoostEvent status
// 	// Matching by name or label
// 	boost, _ := kubeapi.GetBoostEvent(svc.Name, svc.Namespace)

// 	// 4. PATCH: Create the JSON patch to override CPU
// 	patch := fmt.Sprintf(`[{"op": "replace", "path": "/spec/template/spec/containers/0/resources/limits/cpu", "value": "%s"}]`, boost.Status.RunningCPU)

// 	// 5. Respond to K8s
// 	response := v1.AdmissionResponse{
// 		Allowed:   true,
// 		Patch:     []byte(patch),
// 		PatchType: func() *v1.PatchType { pt := v1.PatchTypeJSONPatch; return &pt }(),
// 	}
// 	// Send response back...
// }
