package watcher

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"

	"recon/api/kubeapi"
	"recon/api/logging"
)

// StartKsvcWatcher watches Knative Services cluster-wide. On each Added
// event it patches the new ksvc with the RunningCPU of any completed Recon
// whose selector lists this ksvc name, closing the propagation gap for
// ksvcs created after the binary search has already finished.
//
// PatchResourceLimits is an idempotent JSON-patch replace, so re-applying
// it to ksvcs that RunWorker already patched (e.g. on the initial watch
// replay at controller startup) is harmless.
func (bw *BoostWatcher) StartKsvcWatcher(ctx context.Context) {
	w, err := DYNCLIENT.Resource(KSVC_GVR).
		Namespace(metav1.NamespaceAll).
		Watch(ctx, metav1.ListOptions{})
	if err != nil {
		logging.Failure("ksvc watcher: failed to start:", err)
		return
	}
	defer w.Stop()

	logging.Stage("Ksvc watcher started: propagating RunningCPU to new ksvcs...")

	for {
		select {
		case <-ctx.Done():
			logging.Stage("Ksvc watcher stopping: context cancelled.")
			return
		case event, ok := <-w.ResultChan():
			if !ok {
				return
			}
			if event.Type != watch.Added {
				continue
			}
			ksvc, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			bw.maybePatchNewKsvc(ctx, ksvc)
		}
	}
}

func (bw *BoostWatcher) maybePatchNewKsvc(ctx context.Context, ksvc *unstructured.Unstructured) {
	ns := ksvc.GetNamespace()
	name := ksvc.GetName()

	bw.mu.RLock()
	defer bw.mu.RUnlock()

	for _, recon := range bw.completed {
		if recon.Metadata.Namespace != ns {
			continue
		}
		if len(recon.Selector.MatchExpressions) == 0 {
			continue
		}
		for _, v := range recon.Selector.MatchExpressions[0].Values {
			if v != name {
				continue
			}
			logging.Info("Propagating RunningCPU to new ksvc:",
				ns+"/"+name, "->", recon.RunningCPU)
			if err := kubeapi.PatchResourceLimits(ctx, ns, name, recon.RunningCPU); err != nil {
				logging.Failure("ksvc watcher: patch failed:", err)
			}
			return
		}
	}
}
