package watcher

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"

	"nimbus/api/kubeapi"
	"nimbus/api/logging"
)

// StartKsvcWatcher watches Knative Services cluster-wide. On each Added
// event it patches the new ksvc with the RunningCPU of any completed Nimbus
// whose selector lists this ksvc name, closing the propagation gap for
// ksvcs created after the binary search has already finished.
//
// PatchResourceLimits is an idempotent JSON-patch replace, so re-applying
// it to ksvcs that RunWorker already patched (e.g. on the initial watch
// replay at controller startup) is harmless.
func (nw *NimbusWatcher) StartKsvcWatcher(ctx context.Context) {
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
			nw.maybePatchNewKsvc(ctx, ksvc)
		}
	}
}

func (nw *NimbusWatcher) maybePatchNewKsvc(ctx context.Context, ksvc *unstructured.Unstructured) {
	ns := ksvc.GetNamespace()
	name := ksvc.GetName()

	nw.mu.RLock()
	defer nw.mu.RUnlock()

	for _, nimbus := range nw.completed {
		if nimbus.Metadata.Namespace != ns {
			continue
		}
		if len(nimbus.Selector.MatchExpressions) == 0 {
			continue
		}
		for _, v := range nimbus.Selector.MatchExpressions[0].Values {
			if v != name {
				continue
			}
			runningMax := kubeapi.MaxRunningCpu(nimbus.PerNodeResults)
			if runningMax == "" {
				logging.Warning("ksvc watcher: skipping propagation, no per-node results yet for", ns+"/"+name)
				return
			}
			// Re-assert both the pool nodeSelector and the RunningCPU so a
			// ksvc applied after the Nimbus completed still ends up with
			// the same placement and CPU contract as ksvcs that existed
			// at offline-start. Without the nodeSelector step a late-
			// arriving ksvc would carry whatever the user wrote in its
			// own manifest, violating the "Nimbus is the source of truth
			// for nodeSelector" invariant declared in offline.md §3.0.
			if len(nimbus.Spec.Placement.NodeSelector) > 0 {
				if err := kubeapi.PatchKsvcNodeSelector(ctx, ns, name, nimbus.Spec.Placement.NodeSelector); err != nil {
					logging.Failure("ksvc watcher: nodeSelector patch failed:", err)
					return
				}
			}
			logging.Info("Propagating RunningCPU to new ksvc:",
				ns+"/"+name, "->", runningMax)
			if err := kubeapi.PatchResourceLimits(ctx, ns, name, runningMax); err != nil {
				logging.Failure("ksvc watcher: patch failed:", err)
			}
			return
		}
	}
}
