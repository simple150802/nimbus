package watcher

import (
	"context"
	"fmt"

	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"

	"recon/api/algorithm"
	"recon/api/boostevent"
	"recon/api/logging"
)

// 2. The Producer
func (bw *BoostWatcher) StartWatcher() {
	watcher, err := DYNCLIENT.Resource(ADV_GVR).Namespace(metav1.NamespaceAll).Watch(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logging.Failure("Error starting watcher:", err)
		return
	}
	defer watcher.Stop()

	logging.Stage("Watcher started: Listening for K8s events across ALL namespaces...")

	for event := range watcher.ResultChan() {
		obj, ok := event.Object.(*unstructured.Unstructured)
		if !ok {
			continue
		}

		var payload boostevent.BoostEvent

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
	logging.Stage("Worker started: Direct linked-list monitoring with RLock...")

	var current *boostevent.BoostEvent

	for {
		bw.mu.RLock()
		if bw.head == nil {
			logging.Normal("List is empty, waiting...")
			current = nil
			bw.mu.RUnlock()
			time.Sleep(2 * time.Second)
			continue
		}

		if current == nil {
			current = bw.head
		}

		if current.StartingSaturated == true && current.RunningSaturated == true {
			continue
		}
		if current.StartingSaturated == false && current.RunningSaturated == true || current.StartingSaturated == true && current.RunningSaturated == false {
			logging.Failure("VERY CRITICAL ERR OCCUR !!!")
			return
		}

		// 3. Now we can safely print data
		logging.Stage("STEP PROCESSING:", current.Metadata.Namespace, current.Metadata.Name)
		algorithm.BinarySearch(context.TODO(), current)

		bw.mu.RUnlock()
		time.Sleep(2 * time.Second)
	}
}
