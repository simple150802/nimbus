package kubeapi

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MonitorKsvcResources polls a Knative service's pods every 2 seconds to print their resource specs.
func MonitorKsvcResources(ctx context.Context, namespace string, ksvcName string) {
	// 1. Define the Knative specific label selector
	labelSelector := fmt.Sprintf("serving.knative.dev/service=%s", ksvcName)

	// 2. Create a Ticker that fires every 2 seconds
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop() // Best practice: always stop tickers to prevent memory leaks

	fmt.Printf("Starting resource monitor for ksvc '%s' in namespace '%s'...\n", ksvcName, namespace)

	for {
		select {
		case <-ctx.Done():
			// 3. The "Stop" Trigger! If the context is cancelled, exit the loop.
			fmt.Println("\n[Monitor] Received stop signal. Shutting down.")
			return

		case <-ticker.C:
			// 4. This executes every 2 seconds
			pods, err := CLIENTSET.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
				LabelSelector: labelSelector,
			})

			if err != nil {
				fmt.Printf("[Error] Failed to fetch pods: %v\n", err)
				continue // Don't crash, just try again on the next tick
			}

			fmt.Printf("\n--- Polling at %s ---\n", time.Now().Format("15:04:05"))
			if len(pods.Items) == 0 {
				fmt.Println("No pods currently running for this ksvc (scaled to 0).")
				continue
			}

			for _, pod := range pods.Items {
				fmt.Printf("Pod: %s\n", pod.Name)
				container := pod.Spec.Containers[0]

				// Note: Knative injects a 'queue-proxy' container alongside your app.
				// You will see resources for both here. Your app is usually called 'user-container'.
				fmt.Printf("  Container: %s\n", container.Name)

				cpuReq := container.Resources.Requests.Cpu().String()
				memReq := container.Resources.Requests.Memory().String()
				cpuLim := container.Resources.Limits.Cpu().String()
				memLim := container.Resources.Limits.Memory().String()

				fmt.Printf("    Requests: CPU=%s, Memory=%s\n", cpuReq, memReq)
				fmt.Printf("    Limits:   CPU=%s, Memory=%s\n", cpuLim, memLim)
			}
		}
	}
}
