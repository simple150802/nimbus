package watcher

import (
	"context"
	"fmt"
	"log"

	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

type BoostWatcher struct {
	client dynamic.Interface
	gvr    schema.GroupVersionResource

	// ==========================================
	// 🔗 YOUR NEW CUSTOM QUEUE
	// ==========================================
	head *BoostEvent // Start of the queue (the next item to be processed)
	tail *BoostEvent // End of the queue (where brand new items are added)

	// 🔒 The Lock: Prevents the Watcher and Worker from crashing the app
	mu sync.RWMutex
}

func (bw *BoostWatcher) Enqueue(newEvent *BoostEvent) {
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
func (bw *BoostWatcher) Dequeue(target *BoostEvent) *BoostEvent {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	// 1. If the list is empty, there is nothing to remove.
	if bw.head == nil {
		return nil
	}

	// ==========================================
	// CASE A: The target is at the very front (the head)
	// ==========================================
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

	// ==========================================
	// CASE B: The target is in the middle or at the end
	// ==========================================
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

func NewBoostWatcher(client dynamic.Interface, gvr schema.GroupVersionResource) *BoostWatcher {
	return &BoostWatcher{
		client: client,
		gvr:    gvr,
	}
}

// 2. The Producer
func (bw *BoostWatcher) StartWatcher() {
	watcher, err := bw.client.Resource(bw.gvr).Namespace(metav1.NamespaceAll).Watch(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Fatalf("Error starting watcher: %v", err)
	}
	defer watcher.Stop()

	fmt.Println("👀 Watcher started: Listening for K8s events across ALL namespaces...")

	for event := range watcher.ResultChan() {
		obj, ok := event.Object.(*unstructured.Unstructured)
		if !ok {
			continue
		}

		var payload BoostEvent

		err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &payload)
		if err != nil {
			fmt.Printf("Error converting object: %v\n", err)
			continue
		}

		// Route the event type using your new Enqueue method!
		switch event.Type {
		case watch.Added:
			bw.Enqueue(&payload)
		case watch.Deleted:
			bw.Dequeue(&payload)
		}
	}
}

// 3. The Consumer
func (bw *BoostWatcher) RunWorker() {
	fmt.Println("👷 Worker started: Direct linked-list monitoring with RLock...")

	current := bw.head

	for {
		fmt.Printf("\n🔄 RECONCILIATION: Walking list from head to tail...\n")

		bw.mu.RLock()
		if bw.head == nil {
			fmt.Println("📭 List is empty, waiting...")
			bw.mu.RUnlock()
			time.Sleep(5 * time.Second)
			continue
		}
		fmt.Printf("\n🛠️  STEP PROCESSING: %s/%s\n", current.Metadata.Namespace, current.Metadata.Name)
		if current.Next != bw.tail {
			current = current.Next
		}
		if current.Next == bw.tail {
			current = bw.head
		}
		bw.mu.RUnlock()
		time.Sleep(2 * time.Second)

	}
}

// reconcileBoost handles the actual "work" for a single item
func (bw *BoostWatcher) reconcileBoost(item *BoostEvent) {
	// 🚀 Put your HTTP POST logic here.
	// Even if this takes 3 seconds, the Watcher is NOT blocked!
	fmt.Printf("      -> [API CALL] Sending heartbeat for %s\n", item.Metadata.Name)
}
