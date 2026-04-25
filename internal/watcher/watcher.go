package watcher

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"

	"nimbus/api/algorithm"
	"nimbus/api/nimbusevent"
	"nimbus/api/kubeapi"
	"nimbus/api/logging"
)

// 2. The Producer
func (bw *NimbusWatcher) StartWatcher(ctx context.Context) {
	w, err := DYNCLIENT.Resource(NIMBUS_GVR).
		Namespace(metav1.NamespaceAll).
		Watch(ctx, metav1.ListOptions{})
	if err != nil {
		logging.Failure("Error starting watcher:", err)
		return
	}
	defer w.Stop()

	logging.Stage("Watcher started: Listening for Nimbus events across ALL namespaces...")

	for {
		select {
		case <-ctx.Done():
			logging.Stage("Watcher stopping: context cancelled.")
			return
		case event, ok := <-w.ResultChan():
			if !ok {
				return
			}
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			var payload nimbusevent.NimbusEvent
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &payload); err != nil {
				logging.Failure("error converting Nimbus object:", err)
				continue
			}
			switch event.Type {
			case watch.Added:
				bw.Enqueue(&payload)
			case watch.Deleted:
				bw.Dequeue(&payload)
			}
		}
	}
}

// 3. The Consumer
func (bw *NimbusWatcher) RunWorker(ctx context.Context) {
	logging.Stage("Worker started: Direct linked-list monitoring with RLock...")

	// emptyLogged stops the "List is empty, waiting..." line from spamming
	// every 2 s while the queue is idle. Reset whenever a new Nimbus arrives.
	emptyLogged := false

	for {
		select {
		case <-ctx.Done():
			logging.Stage("Worker stopping: context cancelled.")
			return
		default:
		}

		bw.mu.RLock()
		current := bw.head
		bw.mu.RUnlock()

		if current == nil {
			if !emptyLogged {
				logging.Normal("List is empty, waiting...")
				emptyLogged = true
			}
			if !sleepOrDone(ctx, 2*time.Second) {
				return
			}
			continue
		}
		emptyLogged = false

		// Fast path: if the Nimbus's .status already carries finalized CPU
		// values, skip the binary search entirely. This avoids re-probing on
		// controller restart and, more importantly, prevents the boost
		// controller's "never lower" semantic from biasing subsequent runs.
		if current.Status.StartingCpu != "" && current.Status.RunningCpu != "" {
			logging.Info("Skipping binary search — Nimbus already completed:",
				current.Metadata.Namespace+"/"+current.Metadata.Name,
				"starting=", current.Status.StartingCpu,
				"running=", current.Status.RunningCpu)
			current.StartingCPU = current.Status.StartingCpu
			current.RunningCPU = current.Status.RunningCpu
			current.StartingSaturated = true
			current.RunningSaturated = true
		} else {
			// Precondition: all target ksvcs must exist before we probe.
			// Without this we'd run the whole search against a missing
			// target, every getResptCold/Warm would fail, and we'd persist
			// garbage CPU values to status. Leave the Nimbus in the queue
			// so the next tick retries once the user applies the ksvc.
			if missing := bw.missingTargetKsvcs(ctx, current); len(missing) > 0 {
				logging.Warning("Waiting for target ksvc(s) to appear in namespace",
					current.Metadata.Namespace, missing)
				if !sleepOrDone(ctx, 2*time.Second) {
					return
				}
				continue
			}

			logging.Stage("STEP PROCESSING:", current.Metadata.Namespace, current.Metadata.Name)
			if _, err := algorithm.BinarySearch(ctx, current); err != nil {
				logging.Failure("BinarySearch aborted — leaving Nimbus in queue to retry:", err)
				if !sleepOrDone(ctx, 2*time.Second) {
					return
				}
				continue
			}

			// Persist finalized values so the next Added event (restart,
			// re-apply) takes the fast path above.
			if err := kubeapi.WriteNimbusStatus(ctx,
				current.Metadata.Namespace, current.Metadata.Name,
				current.StartingCPU, current.RunningCPU); err != nil {
				logging.Failure("Failed to persist Nimbus status:", err)
			}
		}

		// Apply (idempotent). Both code paths converge here so ksvcs get
		// the StartupCPUBoost CR and the running-phase CPU limit.
		kubeapi.CreateStartupCPUBoost(ctx, current, current.StartingCPU)
		for _, ksvc := range current.Selector.MatchExpressions[0].Values {
			kubeapi.PatchResourceLimits(ctx, current.Metadata.Namespace, ksvc, current.RunningCPU)
		}

		// Register so the ksvc watcher can propagate RunningCPU to future
		// ksvcs matching this Nimbus's selector.
		bw.mu.Lock()
		key := current.Metadata.Namespace + "/" + current.Metadata.Name
		bw.completed[key] = current
		bw.mu.Unlock()

		// Remove this Nimbus from the queue — it's done. Without this the
		// worker would spin on the same saturated head forever.
		bw.Dequeue(current)

		if !sleepOrDone(ctx, 2*time.Second) {
			return
		}
	}
}

// sleepOrDone waits for d or for ctx to be cancelled, whichever comes first.
// Returns false if ctx was cancelled.
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// missingTargetKsvcs returns the subset of the Nimbus's selector values whose
// Knative service does not currently exist in the Nimbus's namespace. An empty
// result means all targets are present and the binary search can proceed.
// Any error from the API (NotFound, forbidden, transient) is treated as
// "missing" — the worker will retry on the next tick.
func (bw *NimbusWatcher) missingTargetKsvcs(ctx context.Context, ev *nimbusevent.NimbusEvent) []string {
	if len(ev.Selector.MatchExpressions) == 0 {
		return nil
	}
	var missing []string
	for _, name := range ev.Selector.MatchExpressions[0].Values {
		_, err := DYNCLIENT.Resource(KSVC_GVR).
			Namespace(ev.Metadata.Namespace).
			Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			missing = append(missing, name)
		}
	}
	return missing
}
