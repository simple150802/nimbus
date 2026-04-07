package algorithm

import (
	"context"
	"fmt"
	"io"
	"lazyken-controller/api/boostevent"
	"lazyken-controller/api/kubeapi"
	"lazyken-controller/api/kubeconfig"
	"lazyken-controller/api/logging"
	"net/http"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	DYNCLIENT = kubeconfig.DYNCLIENT
	CLIENTSET = kubeconfig.CLIENTSET
	STD_GVR   = kubeconfig.STD_GVR
	ADV_GVR   = kubeconfig.ADV_GVR
)

func getResptCold(ctx context.Context, event *boostevent.BoostEvent, cpuValue string) (time.Duration, error) {
	logging.Stage("Measuring response time cold on", cpuValue, "CPU")
	// 2. Build the Label Selector String dynamically from the CRD
	var selectorParts []string
	for _, expr := range event.Selector.MatchExpressions {
		vals := strings.Join(expr.Values, ",")

		part := fmt.Sprintf("%s %s (%s)", expr.Key, strings.ToLower(expr.Operator), vals)
		selectorParts = append(selectorParts, part)
	}

	// Join all expressions together with commas (creates a logical AND for the label selector)
	labelSelector := strings.Join(selectorParts, ",")

	deployments, err := CLIENTSET.AppsV1().Deployments(event.Metadata.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})

	if err != nil {
		logging.Failure("Failed to list deployments:", err)
		return 0, err // Abort on API error
	}

	// If the array is empty, the app hasn't been applied yet!
	if len(deployments.Items) == 0 {
		logging.Warning("Target resource not found! Aborting test for this event.")
		return 0, err
	}

	logging.Warning("Waiting for pods to scale down to 0...")

	for {
		pods, err := CLIENTSET.CoreV1().Pods(event.Metadata.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labelSelector,
		})

		if err != nil {
			logging.Failure("Failed to list pods:", err)
			return 0, err // Exit the function if there's a critical API error
		}

		// Check if we hit our target state!
		if len(pods.Items) == 0 {
			logging.Success("Pods successfully scaled to 0!")
			break // Break out of the infinite loop
		}

		// Log the current count so you can see it working in the terminal
		// logging.Normal("Still waiting... Found", len(pods.Items), "pods running.")

		// Sleep for 2 seconds before asking the API again
		time.Sleep(2 * time.Second)
	}

	// Apply CRD here
	kubeapi.CreateStartupCPUBoost(ctx, event, cpuValue)

	stop_ctx, cancel := context.WithCancel(context.Background())
	go kubeapi.MonitorKsvcResources(stop_ctx, "serverless", "measure-yolo")

	// Guarantee the CRD is deleted when this function finishes!
	defer kubeapi.DeleteStartupCPUBoost(ctx, event.Metadata.Namespace, event.Metadata.Name)

	responseTime, err := triggerHttp(event.Spec.DurationPolicy.ApiCondition)
	logging.Normal("Final recorded cold start time was:", responseTime)

	time.Sleep(2 * time.Second)
	test, err := triggerHttp(event.Spec.DurationPolicy.ApiCondition)
	logging.Normal("Test1:", test)

	time.Sleep(2 * time.Second)
	test2, err := triggerHttp(event.Spec.DurationPolicy.ApiCondition)
	logging.Normal("Test2:", test2)

	time.Sleep(2 * time.Second)
	test3, err := triggerHttp(event.Spec.DurationPolicy.ApiCondition)
	logging.Normal("Test3:", test3)

	cancel()

	if err != nil {
		logging.Failure("Failed to measure response time correctly:", err)
		return 0, err
	}

	logging.Normal("Final recorded cold start time was:", responseTime)
	return responseTime, nil

}

func getResptWarm(ctx context.Context, event *boostevent.BoostEvent, cpuValue string, cpuCold string) (time.Duration, error) {
	logging.Stage("Measuring response time warm on", cpuValue, "CPU")
	// 2. Build the Label Selector String dynamically from the CRD
	var selectorParts []string
	for _, expr := range event.Selector.MatchExpressions {
		vals := strings.Join(expr.Values, ",")

		part := fmt.Sprintf("%s %s (%s)", expr.Key, strings.ToLower(expr.Operator), vals)
		selectorParts = append(selectorParts, part)
	}

	// Join all expressions together with commas (creates a logical AND for the label selector)
	labelSelector := strings.Join(selectorParts, ",")

	deployments, err := CLIENTSET.AppsV1().Deployments(event.Metadata.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		logging.Failure("Failed to list deployments:", err)
		return 0, err // Abort on API error
	}
	// If the array is empty, the app hasn't been applied yet!
	if len(deployments.Items) == 0 {
		logging.Warning("Target resource not found! Aborting test for this event.")
		return 0, err
	}

	ksvcName := "measure-yolo"

	for _, d := range deployments.Items {
		// 1. Log the current state from the existing Deployment
		for _, container := range d.Spec.Template.Spec.Containers {
			if container.Name == "user-container" {
				oldCPU := container.Resources.Limits.Cpu()
				logging.Info("Requesting Knative update for ", container.Name, " from ", oldCPU.String(), " to ", cpuValue)
			}
		}

		// 2. Patch the KSVC instead of the Deployment
		// This creates a NEW revision and a NEW deployment
		err := kubeapi.PatchResourceLimits(ctx, d.Namespace, ksvcName, cpuValue)
		if err != nil {
			logging.Failure("Failed to patch Knative Service: ", err)
			continue
		}
		logging.Info("Successfully triggered new Knative Revision for: ", ksvcName)
	}

	logging.Warning("Waiting for pods to scale down to 0...")

	for {
		pods, err := CLIENTSET.CoreV1().Pods(event.Metadata.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labelSelector,
		})

		if err != nil {
			logging.Failure("Failed to list pods:", err)
			return 0, err // Exit the function if there's a critical API error
		}

		// Check if we hit our target state!
		if len(pods.Items) == 0 {
			logging.Success("Pods successfully scaled to 0!")
			break // Break out of the infinite loop
		}

		// Log the current count so you can see it working in the terminal
		// logging.Normal("Still waiting... Found", len(pods.Items), "pods running.")

		// Sleep for 2 seconds before asking the API again
		time.Sleep(2 * time.Second)
	}

	// Apply CRD here
	kubeapi.CreateStartupCPUBoost(ctx, event, cpuCold)

	stop_ctx, cancel := context.WithCancel(context.Background())
	go kubeapi.MonitorKsvcResources(stop_ctx, "serverless", "measure-yolo")

	// Guarantee the CRD is deleted when this function finishes!
	defer kubeapi.DeleteStartupCPUBoost(ctx, event.Metadata.Namespace, event.Metadata.Name)

	_, err = triggerHttp(event.Spec.DurationPolicy.ApiCondition)
	if err != nil {
		logging.Failure("Failed to measure response time correctly:", err)
		cancel()
		return 0, err
	}

	var sum time.Duration
	for i := 0; i < 10; i++ {
		// 2. Trigger the HTTP call
		responseTimeWarm, err := triggerHttp(event.Spec.DurationPolicy.ApiCondition)

		if err != nil {
			// Log the error and move to the next step, or handle as needed
			fmt.Printf("Step %d failed: %v\n", i+1, err)
			i -= 1
		} else {
			// 3. Store the result (assuming responseTimeCold is float64)
			sum += responseTimeWarm
		}

		// 4. Wait for 2 seconds before the next iteration (except after the last one)
		time.Sleep(2 * time.Second)
	}

	cancel()

	avg := sum / 10

	logging.Normal("Final recorded warm response time was:", avg)
	return avg, nil
}

func triggerHttp(api_condition boostevent.ApiCondition) (time.Duration, error) {
	targetURL := api_condition.Url
	expectedResponse := api_condition.Response

	logging.Normal("Triggering pod... Sending request to:", targetURL)

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	start := time.Now()

	for {
		resp, err := client.Get(targetURL)
		if err != nil {
			// Usually means the Service/Pod isn't reachable yet (Normal during cold start)
			logging.Normal("Pod not reachable yet, retrying...")
			time.Sleep(2 * time.Second)
			continue
		}

		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			logging.Warning("Failed to read response body, retrying...")
			time.Sleep(2 * time.Second)
			continue
		}

		bodyString := string(bodyBytes)
		duration := time.Since(start)

		// Check if the body contains our expected string
		if strings.Contains(bodyString, expectedResponse) {
			logging.Success(fmt.Sprintf("Receive response successful! Expected response received in %v", duration))
			return duration, nil
		}

	}
}
