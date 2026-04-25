package watcher

import (
	"recon/api/boostevent"
	"sync"
)

type BoostWatcher struct {
	head *boostevent.BoostEvent // Start of the queue (the next item to be processed)
	tail *boostevent.BoostEvent // End of the queue (where brand new items are added)

	// 🔒 The Lock: Prevents the Watcher and Worker from crashing the app
	mu sync.RWMutex

	// completed holds Recons whose RunningCPU is finalized, keyed by
	// "<namespace>/<name>". The ksvc watcher reads it to decide whether to
	// propagate RunningCPU to a newly-created ksvc. Protected by mu.
	completed map[string]*boostevent.BoostEvent
}

func (bw *BoostWatcher) Enqueue(newEvent *boostevent.BoostEvent) {
	// 1. Lock the queue so the Worker can't pull from it while we are working
	bw.mu.Lock()
	defer bw.mu.Unlock()

	// 2. If the queue is totally empty, this new event is BOTH the head and the tail
	if bw.head == nil {
		bw.head = newEvent
		bw.tail = newEvent
		return
	}

	// 3. If the queue has items, tell the current tail to point to this new event...
	bw.tail.Next = newEvent
	bw.tail = newEvent
}

// Dequeue safely removes and returns the event at the front of the line.
// It returns nil if the queue is empty.
// Dequeue searches the list for a CRD that matches the target's Namespace and Name.
// It safely severs it from the linked list and returns it.
func (bw *BoostWatcher) Dequeue(target *boostevent.BoostEvent) *boostevent.BoostEvent {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	// 1. If the list is empty, there is nothing to remove.
	if bw.head == nil {
		return nil
	}

	if bw.head.Metadata.Namespace == target.Metadata.Namespace && bw.head.Metadata.Name == target.Metadata.Name {
		item := bw.head
		bw.head = bw.head.Next // Move the head to the next person in line

		// If the list is now empty, reset the tail too
		if bw.head == nil {
			bw.tail = nil
		}

		item.Next = nil // Sever connection for memory safety
		return item
	}

	current := bw.head

	for current.Next != nil {
		// Look ahead to see if the NEXT node is our target
		if current.Next.Metadata.Namespace == target.Metadata.Namespace && current.Next.Metadata.Name == target.Metadata.Name {
			item := current.Next

			// 🪄 Stitch the chain together, bypassing the target item!
			current.Next = item.Next

			// If the item we just removed was the tail, the current node becomes the new tail
			if item == bw.tail {
				bw.tail = current
			}

			item.Next = nil // Sever connection
			return item
		}

		current = current.Next
	}

	// 3. Not found in the list
	return nil
}

func NewBoostWatcher() *BoostWatcher {
	return &BoostWatcher{
		completed: make(map[string]*boostevent.BoostEvent),
	}
}
