package kubeconfig

import (
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	// ---------------------------------------------------------
	// 1. GLOBAL GVR VARIABLES
	// ---------------------------------------------------------
	ADV_GVR = schema.GroupVersionResource{
		Group:    "lazyken.io",
		Version:  "v1alpha1",
		Resource: "recons",
	}

	STD_GVR = schema.GroupVersionResource{
		Group:    "autoscaling.x-k8s.io",
		Version:  "v1alpha1",
		Resource: "startupcpuboosts",
	}

	KSVC_GVR = schema.GroupVersionResource{
		Group:    "serving.knative.dev",
		Version:  "v1",
		Resource: "services",
	}
	// ---------------------------------------------------------
	// 2. GLOBAL CLIENT VARIABLES
	// ---------------------------------------------------------
	RESTCONFIG *rest.Config
	DYNCLIENT  dynamic.Interface
	CLIENTSET  *kubernetes.Clientset
)

// The init() function runs automatically exactly once when your app starts.
// It is the perfect place to set up global variables that might return errors.
func init() {
	var err error

	// 1. Load the Config
	RESTCONFIG, err = getRestConfig()
	if err != nil {
		panic("Failed to load kubeconfig: " + err.Error())
	}

	// 2. Initialize the Global Dynamic Client
	DYNCLIENT, err = dynamic.NewForConfig(RESTCONFIG)
	if err != nil {
		panic("Failed to create Global Dynamic Client: " + err.Error())
	}

	// 3. Initialize the Global Standard Clientset
	CLIENTSET, err = kubernetes.NewForConfig(RESTCONFIG)
	if err != nil {
		panic("Failed to create Global K8s Clientset: " + err.Error())
	}
}

// getRestConfig is now a private helper function used only by init()
func getRestConfig() (*rest.Config, error) {
	config, err := rest.InClusterConfig()
	if err == nil {
		return config, nil
	}

	kubeconfigPath := filepath.Join(os.Getenv("HOME"), ".kube", "config")
	return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
}
