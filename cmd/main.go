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

	bw := watcher.NewNimbusWatcher()

	go bw.StartWatcher(ctx)     // Nimbus CRD producer
	go bw.StartKsvcWatcher(ctx) // ksvc Added propagation
	bw.RunWorker(ctx)           // consumer — blocks until ctx is cancelled

	logging.Stage("Shutdown complete.")
}
