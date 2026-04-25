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
