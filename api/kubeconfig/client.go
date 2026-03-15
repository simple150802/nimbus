package kubeconfig

import (
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

// NewDynamicClient finds the local kubeconfig and returns a ready-to-use dynamic client.
func NewDynamicClient() (dynamic.Interface, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	kubeconfigPath := filepath.Join(homeDir, ".kube", "config")

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, err
	}

	// Return the client and the error (if any) back to the caller
	return dynamic.NewForConfig(config)
}

// GetStartupCPUBoostGVR returns the routing info for your specific CRD.
// You can add more functions like this here as you invent more CRDs!
func GetStartupCPUBoostGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "lazyken.io.vn",
		Version:  "v1alpha1",
		Resource: "startupcpuboosts",
	}
}
