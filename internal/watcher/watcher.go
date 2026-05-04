package watcher

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"

	"nimbus/api/algorithm"
	"nimbus/api/kubeapi"
	"nimbus/api/logging"
	"nimbus/api/nimbusevent"
)

const idleSleep = 2 * time.Second

// StartWatcher is the producer: watches Nimbus CRs cluster-wide and pushes
// Added/Deleted events into the worker queue. Blocks until ctx is cancelled.
func (nw *NimbusWatcher) StartWatcher(ctx context.Context) {
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
				nw.Enqueue(&payload)
			case watch.Deleted:
				nw.Dequeue(&payload)
			}
		}
	}
}

// RunWorker is the consumer: drains the queue head one Nimbus at a time,
// runs (or skips) the binary search, applies the result, and dequeues.
// Blocks until ctx is cancelled.
func (nw *NimbusWatcher) RunWorker(ctx context.Context) {
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

		nw.mu.RLock()
		current := nw.head
		nw.mu.RUnlock()

		if current == nil {
			if !emptyLogged {
				logging.Normal("List is empty, waiting...")
				emptyLogged = true
			}
			if !sleepOrDone(ctx, idleSleep) {
				return
			}
			continue
		}
		emptyLogged = false

		// Defensive — the CRD schema enforces minItems: 1 on both, so this
		// only fires for objects that bypassed validation. Drop the offender
		// rather than panic in `[0]` indexing downstream.
		if err := validateNimbus(current); err != nil {
			logging.Failure("Dropping malformed Nimbus from queue:", err)
			nw.Dequeue(current)
			continue
		}

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
			if missing := nw.missingTargetKsvcs(ctx, current); len(missing) > 0 {
				logging.Warning("Waiting for target ksvc(s) to appear in namespace",
					current.Metadata.Namespace, missing)
				if !sleepOrDone(ctx, idleSleep) {
					return
				}
				continue
			}

			// Discover which nodes the ksvc can run on. Read-only; populates
			// current.CandidateNodes. The list is logged but not yet used
			// downstream — Phase 2 of the multi-node refactor will loop the
			// binary search over each candidate node. See multiple_nodes.md.
			if err := nw.discoverCandidateNodes(ctx, current); err != nil {
				logging.Warning("[nodes] discovery failed — retrying on next tick:", err)
				if !sleepOrDone(ctx, idleSleep) {
					return
				}
				continue
			}

			logging.Stage("STEP PROCESSING:", current.Metadata.Namespace, current.Metadata.Name)
			if _, err := algorithm.BinarySearch(ctx, current); err != nil {
				logging.Failure("BinarySearch aborted — leaving Nimbus in queue to retry:", err)
				if !sleepOrDone(ctx, idleSleep) {
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

		// Apply paths are idempotent. Errors are logged inside each helper;
		// surfacing them here would just duplicate log lines.
		kubeapi.CreateStartupCPUBoost(ctx, current, current.StartingCPU)
		for _, ksvc := range current.Selector.MatchExpressions[0].Values {
			kubeapi.PatchResourceLimits(ctx, current.Metadata.Namespace, ksvc, current.RunningCPU)
		}

		// Register so the ksvc watcher can propagate RunningCPU to future
		// ksvcs matching this Nimbus's selector.
		nw.mu.Lock()
		key := current.Metadata.Namespace + "/" + current.Metadata.Name
		nw.completed[key] = current
		nw.mu.Unlock()

		// Without Dequeue here the worker would spin on the same saturated
		// head forever.
		nw.Dequeue(current)

		if !sleepOrDone(ctx, idleSleep) {
			return
		}
	}
}

// validateNimbus rejects events that would panic the [0]-indexed code
// downstream. The CRD schema already enforces these via `minItems: 1`,
// so this is belt-and-suspenders for objects that somehow bypass it.
func validateNimbus(ev *nimbusevent.NimbusEvent) error {
	if len(ev.Selector.MatchExpressions) == 0 {
		return fmt.Errorf("%s/%s: selector.matchExpressions is empty",
			ev.Metadata.Namespace, ev.Metadata.Name)
	}
	if len(ev.Selector.MatchExpressions[0].Values) == 0 {
		return fmt.Errorf("%s/%s: selector.matchExpressions[0].values is empty",
			ev.Metadata.Namespace, ev.Metadata.Name)
	}
	if len(ev.Spec.ResourcePolicy.ContainerPolicies) == 0 {
		return fmt.Errorf("%s/%s: spec.resourcePolicy.containerPolicies is empty",
			ev.Metadata.Namespace, ev.Metadata.Name)
	}
	return nil
}

// sleepOrDone waits for d or for ctx cancellation, whichever comes first.
// Returns false on cancellation.
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// missingTargetKsvcs returns the subset of the Nimbus's selector values
// whose Knative service does not currently exist in the namespace.
// Any API error (NotFound, forbidden, transient) is treated as "missing"
// — the worker will retry on the next tick.
func (nw *NimbusWatcher) missingTargetKsvcs(ctx context.Context, ev *nimbusevent.NimbusEvent) []string {
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
