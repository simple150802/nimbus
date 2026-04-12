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

// func main() {
//     logging.Stage("Hello from LAZYken! Starting RECON...")

//     myWatcher := watcher.NewBoostWatcher()

//     // 1. The Watcher (Producer) - Background
//     go myWatcher.StartWatcher()

//     // 2. The Worker (Consumer/Brain) - Background
//     go myWatcher.RunWorker()

//     // 3. The Webhook (Guard) - Foreground
//     // This starts the HTTPS server for the Webhook
//     http.HandleFunc("/mutate", webhook.HandleMutation)

//     logging.Stage("Webhook Server starting on :9443...")
//     // Note: In 2026, K8s requires TLS for webhooks!
//     server := &http.Server{Addr: ":9443"}
//     err := server.ListenAndServeTLS("cert.pem", "key.pem")
//     if err != nil {
//         logging.Failure("Webhook failed:", err)
//     }
// }
