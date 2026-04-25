package main

import (
	"context"
	"os/signal"
	"syscall"

	"recon/api/logging"
	"recon/internal/watcher"
)

func main() {
	logging.Stage("Hello from LAZYken! Starting RECON...")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	bw := watcher.NewBoostWatcher()

	go bw.StartWatcher(ctx)     // Recon CRD producer
	go bw.StartKsvcWatcher(ctx) // ksvc Added propagation
	bw.RunWorker(ctx)           // consumer — blocks until ctx is cancelled

	logging.Stage("Shutdown complete.")
}
