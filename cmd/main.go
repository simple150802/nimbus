package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"nimbus/api/logging"
	"nimbus/internal/online"
	"nimbus/internal/watcher"
)

// defaultBurstEnvFile seeds the online burst/budget tunables. Override with
// NIMBUS_BURST_ENV_FILE; explicitly-exported env vars win over the file.
const defaultBurstEnvFile = "config/burst.env"

func main() {
	logging.Stage("Hello from LAZYken! Starting NIMBUS...")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Seed burst/budget config before the online goroutines read os.Getenv.
	envFile := defaultBurstEnvFile
	if v := os.Getenv("NIMBUS_BURST_ENV_FILE"); v != "" {
		envFile = v
	}
	online.LoadEnvFile(envFile)

	nw := watcher.NewNimbusWatcher()
	bs := online.NewBurstState() // cluster-wide cold-start-rate signal

	go nw.StartWatcher(ctx)                  // offline: Nimbus CRD producer
	go nw.StartKsvcWatcher(ctx)              // offline: ksvc Added propagation
	go online.StartBurstDecay(ctx, bs)       // online: burst decay loop (fed by /decide)
	go online.StartDecideServer(ctx, nw, bs) // online: synchronous /decide RPC (primary)
	go online.StartController(ctx, nw, bs)   // online: polling waterfall (self-healing fallback)
	nw.RunWorker(ctx)                        // offline: consumer — blocks until ctx is cancelled

	logging.Stage("Shutdown complete.")
}
