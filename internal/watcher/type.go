package watcher

import (
	"sync"

	"nimbus/api/nimbusevent"
)

// NimbusWatcher owns the in-memory queue of pending Nimbus events plus
// the map of completed ones used by the ksvc watcher to propagate
// RunningCPU. The same RWMutex guards the queue's head/tail pointers and
// the completed map.
type NimbusWatcher struct {
	head *nimbusevent.NimbusEvent
	tail *nimbusevent.NimbusEvent

	mu sync.RWMutex

	// completed: keyed by "<namespace>/<name>". Read by the ksvc watcher
	// when deciding whether to propagate RunningCPU to a newly-created
	// ksvc. Protected by mu.
	completed map[string]*nimbusevent.NimbusEvent
}

func NewNimbusWatcher() *NimbusWatcher {
	return &NimbusWatcher{
		completed: make(map[string]*nimbusevent.NimbusEvent),
	}
}

// Enqueue appends a Nimbus event to the tail of the queue. Idempotent on
// (namespace, name) — a duplicate from a watch resync is silently
// dropped so the worker doesn't process the same Nimbus twice.
func (nw *NimbusWatcher) Enqueue(newEvent *nimbusevent.NimbusEvent) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	for cur := nw.head; cur != nil; cur = cur.Next {
		if matches(cur, newEvent) {
			return
		}
	}

	if nw.head == nil {
		nw.head = newEvent
		nw.tail = newEvent
		return
	}
	nw.tail.Next = newEvent
	nw.tail = newEvent
}

// Upsert is the watch.Modified path. It guarantees that a subsequent
// reconcile tick will see the latest Spec/Selector/Status and re-run
// the offline apply loop, so an operator edit (e.g. adding a name to
// selector.values[] or rotating placement.nodeSelector) actually
// converges instead of being silently deduped by Enqueue.
//
// Behaviour:
//   - Removes the entry from `completed` so the worker re-applies on the
//     next tick.
//   - If the Nimbus is already in the queue, replaces its Spec / Selector
//     / Status fields in place (preserving the linked-list pointers) and
//     clears runtime caches so the next tick re-discovers candidate
//     nodes and re-loads PerNodeResults from .status.
//   - If it is neither queued nor completed, appends to the tail.
//
// Race note: when the worker is mid-tick on this Nimbus, in-place field
// replacement creates a small window where the worker observes the new
// Spec partway through processing. For the thesis POC the consequence
// is at most one stale apply followed by re-reconciliation on the next
// watch event — we accept it rather than copy the event into a per-tick
// snapshot.
func (nw *NimbusWatcher) Upsert(newEvent *nimbusevent.NimbusEvent) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	key := newEvent.Metadata.Namespace + "/" + newEvent.Metadata.Name
	delete(nw.completed, key)

	for cur := nw.head; cur != nil; cur = cur.Next {
		if matches(cur, newEvent) {
			cur.Selector = newEvent.Selector
			cur.Spec = newEvent.Spec
			cur.Status = newEvent.Status
			cur.PerNodeResults = nil
			cur.CandidateNodes = nil
			cur.AllSaturated = false
			cur.ExportRoot = ""
			return
		}
	}

	if nw.head == nil {
		nw.head = newEvent
		nw.tail = newEvent
		return
	}
	nw.tail.Next = newEvent
	nw.tail = newEvent
}

// Dequeue does a linear search for the entry whose (namespace, name)
// matches target, severs it from the linked list, and returns it.
// Returns nil if no match. Despite the name, this is not a FIFO pop —
// it's used both for "this Nimbus was deleted, drop it from the queue"
// and "this Nimbus finished processing, remove it".
func (nw *NimbusWatcher) Dequeue(target *nimbusevent.NimbusEvent) *nimbusevent.NimbusEvent {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	if nw.head == nil {
		return nil
	}

	if matches(nw.head, target) {
		item := nw.head
		nw.head = nw.head.Next
		if nw.head == nil {
			nw.tail = nil
		}
		item.Next = nil
		return item
	}

	for cur := nw.head; cur.Next != nil; cur = cur.Next {
		if matches(cur.Next, target) {
			item := cur.Next
			cur.Next = item.Next
			if item == nw.tail {
				nw.tail = cur
			}
			item.Next = nil
			return item
		}
	}
	return nil
}

func matches(a, b *nimbusevent.NimbusEvent) bool {
	return a.Metadata.Namespace == b.Metadata.Namespace &&
		a.Metadata.Name == b.Metadata.Name
}

// ListCompleted returns defensive-copy snapshots of every completed Nimbus,
// for the online reconciler to read without touching the worker queue or
// racing the worker's in-place field replacements. Each snapshot copies the
// fields the online stage consumes — Metadata, Selector, Spec, Status and
// PerNodeResults. The worker only ever replaces these wholesale (never edits
// a shared slice/map in place), so the copies never observe a torn write.
//
// Read-only contract: the online stage must treat the returned events as
// immutable. It is the sole intended caller (Phase 1+).
func (nw *NimbusWatcher) ListCompleted() []*nimbusevent.NimbusEvent {
	nw.mu.RLock()
	defer nw.mu.RUnlock()

	out := make([]*nimbusevent.NimbusEvent, 0, len(nw.completed))
	for _, ev := range nw.completed {
		if ev == nil {
			continue
		}
		snap := &nimbusevent.NimbusEvent{
			Metadata: ev.Metadata,
			Spec:     ev.Spec,
			Status:   ev.Status,
		}
		if len(ev.Selector.MatchExpressions) > 0 {
			snap.Selector.MatchExpressions = make([]nimbusevent.MatchExpression, len(ev.Selector.MatchExpressions))
			for i, me := range ev.Selector.MatchExpressions {
				cp := me
				cp.Values = append([]string(nil), me.Values...)
				snap.Selector.MatchExpressions[i] = cp
			}
		}
		if len(ev.PerNodeResults) > 0 {
			snap.PerNodeResults = make(map[string]*nimbusevent.NodeResult, len(ev.PerNodeResults))
			for node, r := range ev.PerNodeResults {
				if r == nil {
					continue
				}
				rc := *r
				snap.PerNodeResults[node] = &rc
			}
		}
		out = append(out, snap)
	}
	return out
}
