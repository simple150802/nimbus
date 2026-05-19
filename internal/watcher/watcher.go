package watcher

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"

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

		// Precondition: target ksvcs must exist for both the search and the
		// apply step. Without this the search would crash on a missing
		// target and we'd persist garbage; even on the fast path we still
		// need the ksvc present to PatchResourceLimits / CreateStartupCPUBoost.
		// Leave the Nimbus in the queue so the next tick retries.
		if missing := nw.missingTargetKsvcs(ctx, current); len(missing) > 0 {
			logging.Warning("Waiting for target ksvc(s) to appear in namespace",
				current.Metadata.Namespace, missing)
			if !sleepOrDone(ctx, idleSleep) {
				return
			}
			continue
		}

		// Discovery feeds both the saturation check (does .status.perNode
		// cover every candidate?) and the slow-path loop. Read-only.
		if err := nw.discoverCandidateNodes(ctx, current); err != nil {
			logging.Warning("[nodes] discovery failed — retrying on next tick:", err)
			if !sleepOrDone(ctx, idleSleep) {
				return
			}
			continue
		}

		// Always load partial state from .status into PerNodeResults so the
		// per-node loop can inner-skip already-saturated nodes. The outer
		// flag (current.AllSaturated) is set true only when every candidate
		// is fully saturated, in which case we skip the search entirely.
		loadPerNodeFromStatus(current)

		// Overlay spec.preMeasured.loadFromDir for any candidate node that
		// status didn't already saturate. Status wins over preMeasured.
		// preMeasuredContributed is true iff this overlay added saturation
		// for at least one node — used below to persist status when the
		// fast path fires from a preMeasured-only source.
		preMeasuredContributed := applyPreMeasured(current)

		if current.AllSaturated {
			logging.Info(fmt.Sprintf("Skipping binary search — all %d candidate node(s) saturated: %s/%s",
				len(current.CandidateNodes),
				current.Metadata.Namespace, current.Metadata.Name))
			// When preMeasured was the source of saturation (status was
			// empty or partial before the overlay), persist what NIMBUS is
			// about to apply so `kubectl get nimbus -o yaml` reflects it.
			// Skip when preMeasured didn't contribute — status is already
			// authoritative and rewriting it would be a no-op churn.
			if preMeasuredContributed {
				if err := kubeapi.WriteNimbusStatus(ctx,
					current.Metadata.Namespace, current.Metadata.Name,
					current.PerNodeResults); err != nil {
					logging.Failure("Failed to persist Nimbus status (preMeasured fast path):", err)
				}
			}
		} else {
			logging.Stage("STEP PROCESSING:", current.Metadata.Namespace, current.Metadata.Name)
			if err := nw.runMultiNodeSearch(ctx, current); err != nil {
				logging.Failure("Multi-node search aborted — leaving Nimbus in queue to retry:", err)
				if !sleepOrDone(ctx, idleSleep) {
					return
				}
				continue
			}
			// Outer flag follows inner state — recompute so AllSaturated
			// reflects the loop's outcome (true iff every node finished).
			recomputeAllSaturated(current)

			// Persist all per-node results so the next Added event takes the
			// fast path above. Persisted even on partial completion (some
			// nodes saturated, search aborted on a later one) so progress
			// isn't lost — the next reconcile resumes from the saturated set.
			if err := kubeapi.WriteNimbusStatus(ctx,
				current.Metadata.Namespace, current.Metadata.Name,
				current.PerNodeResults); err != nil {
				logging.Failure("Failed to persist Nimbus status:", err)
			}
		}

		// Collapse per-node results into a single ksvc-wide CPU limit for
		// apply. Max is safe: the slowest node's value sets the floor so no
		// node starves at startup.
		startingMax := kubeapi.MaxStartingCpu(current.PerNodeResults)
		runningMax := kubeapi.MaxRunningCpu(current.PerNodeResults)

		// Apply paths are idempotent. Errors are logged inside each helper;
		// surfacing them here would just duplicate log lines. One boost CR
		// per ksvc — each named "<nimbus>-<ksvc>" with labels identifying
		// the owning Nimbus, so the upstream kube-startup-cpu-boost
		// controller runs an independent boost lifecycle per ksvc and the
		// reset recipe can label-select for cleanup.
		for _, ksvc := range current.Selector.MatchExpressions[0].Values {
			kubeapi.CreateStartupCPUBoost(ctx, current, ksvc, startingMax)
			kubeapi.PatchResourceLimits(ctx, current.Metadata.Namespace, ksvc, runningMax)
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
// downstream. The CRD schema already enforces these via `minItems: 1`
// and `required:`, so this is belt-and-suspenders for objects that
// somehow bypass it. Cold-phase fields keep their existing implicit
// trust (no new Go-side checks beyond what was here before); warm-phase
// fields are new, so we explicitly verify them here too.
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
	if ev.Spec.DurationPolicy.WarmApiCondition.Path == "" {
		return fmt.Errorf("%s/%s: spec.durationPolicy.warmApiCondition.path is empty",
			ev.Metadata.Namespace, ev.Metadata.Name)
	}
	if ev.Spec.DurationPolicy.WarmApiCondition.StatusCode <= 0 {
		return fmt.Errorf("%s/%s: spec.durationPolicy.warmApiCondition.statusCode must be a positive HTTP code",
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
