package main

import (
	"context"
	"os/signal"
	"syscall"

	"nimbus/api/logging"
	"nimbus/internal/watcher"
)

func main() {
	logging.Stage("Hello from LAZYken! Starting NIMBUS...")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	nw := watcher.NewNimbusWatcher()

	go nw.StartWatcher(ctx)     // Nimbus CRD producer
	go nw.StartKsvcWatcher(ctx) // ksvc Added propagation
	nw.RunWorker(ctx)           // consumer — blocks until ctx is cancelled

	logging.Stage("Shutdown complete.")
}
