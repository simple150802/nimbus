package main

import (
	"fmt"
	"log"

	"lazyken-controller/api/kubeconfig"
	"lazyken-controller/internal/watcher"
)

func main() {
	fmt.Println("Hello from LAZYken! Starting Controller...")

	// 1. Setup Client & GVR
	dynClient, err := kubeconfig.NewDynamicClient()
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client: %v", err)
	}
	gvr := kubeconfig.GetStartupCPUBoostGVR()

	// 2. Initialize the Watcher struct
	// Notice we no longer pass a channel in! It's handled internally.
	myWatcher := watcher.NewBoostWatcher(dynClient, gvr)

	// 3. Start the Producer (runs in the background)
	go myWatcher.StartWatcher()

	// 4. Start the Consumer (runs in the foreground, blocking forever)
	myWatcher.RunWorker()
}
