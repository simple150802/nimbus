package main

import (
	"recon/api/logging"
	"recon/internal/watcher"
)

func main() {
	logging.Stage("Hello from LAZYken! Starting RECON...")

	// 4. Initialize the Watcher struct with BOTH clients
	myWatcher := watcher.NewBoostWatcher()

	// 5. Start the Producer (runs in the background)
	go myWatcher.StartWatcher()

	// 6. Start the Consumer (runs in the foreground, blocking forever)
	myWatcher.RunWorker()
}
