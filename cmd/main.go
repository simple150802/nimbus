package main

import (
	"lazyken-controller/api/logging"
	"lazyken-controller/internal/watcher"
)

func main() {
	logging.Stage("Hello from LAZYken! Starting Controller...")

	// 4. Initialize the Watcher struct with BOTH clients
	myWatcher := watcher.NewBoostWatcher()

	// 5. Start the Producer (runs in the background)
	go myWatcher.StartWatcher()

	// 6. Start the Consumer (runs in the foreground, blocking forever)
	myWatcher.RunWorker()
}
