package kubeconfig

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// GVR identifiers used across the controller. The Resource string for
// NIMBUS_GVR must match the CRD's spec.names.plural.
var (
	NIMBUS_GVR = schema.GroupVersionResource{
		Group:    "lazyken.io",
		Version:  "v1alpha1",
		Resource: "nimbuses",
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

	// Shared rest config and clients populated at process start.
	// Other packages re-export these as locals so they don't have to
	// prefix every call with `kubeconfig.`.
	RESTCONFIG *rest.Config
	DYNCLIENT  dynamic.Interface
	CLIENTSET  *kubernetes.Clientset
)

// init populates RESTCONFIG / DYNCLIENT / CLIENTSET before main runs.
// It must run at init time because every other package's utils.go
// captures these globals into package-level vars during its own init.
//
// Failure prints a friendly message to stderr and exits 1 (instead of
// panicking with a stack trace) — there's no useful recovery path if
// the controller can't reach the API server.
func init() {
	var err error

	if RESTCONFIG, err = loadRestConfig(); err != nil {
		fail("failed to load kubeconfig", err)
	}
	if DYNCLIENT, err = dynamic.NewForConfig(RESTCONFIG); err != nil {
		fail("failed to create dynamic client", err)
	}
	if CLIENTSET, err = kubernetes.NewForConfig(RESTCONFIG); err != nil {
		fail("failed to create kubernetes clientset", err)
	}
}

// loadRestConfig prefers in-cluster config, falling back to the user's
// kubeconfig file at $HOME/.kube/config. Used for local-dev scenarios.
func loadRestConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	return clientcmd.BuildConfigFromFlags("", filepath.Join(os.Getenv("HOME"), ".kube", "config"))
}

func fail(msg string, err error) {
	fmt.Fprintf(os.Stderr, "nimbus: %s: %v\n", msg, err)
	os.Exit(1)
}
