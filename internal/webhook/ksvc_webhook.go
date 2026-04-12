package webhook

// import (
// 	"context"
// 	"encoding/json"

// 	"k8s.io/apimachinery/pkg/api/resource"
// 	"k8s.io/apimachinery/pkg/types"
// 	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
// )

// func (h *MyHandler) Handle(ctx context.Context, req admission.Request) admission.Response {
// 	// 1. Decode the Knative Service being created
// 	svc := &knativev1.Service{}
// 	h.decoder.Decode(req, svc)

// 	// 2. Fetch the BoostEvent that governs this service
// 	// Tip: Use a label on the Service to know WHICH BoostEvent to fetch
// 	boost := &v1.BoostEvent{}
// 	err := h.Client.Get(ctx, types.NamespacedName{
// 		Name:      "my-boost-calculator",
// 		Namespace: req.Namespace,
// 	}, boost)

// 	if err != nil {
// 		// If we can't find the calculation, let the service deploy with its defaults
// 		return admission.Allowed("BoostEvent not found, using default CPU")
// 	}

// 	// 3. GRAB THE DATA: It's now public and available!
// 	calculatedValue := boost.Status.RunningCPU

// 	// 4. APPLY TO KNATIVE:
// 	if len(svc.Spec.Template.Spec.Containers) > 0 {
// 		svc.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = resource.MustParse(calculatedValue)
// 	}

// 	// 5. Return the patch
// 	marshaledSvc, _ := json.Marshal(svc)
// 	return admission.PatchResponseFromRaw(req.Object.Raw, marshaledSvc)
// }
