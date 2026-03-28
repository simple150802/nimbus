package watcher

import (
	"lazyken-controller/api/kubeconfig"
)

var (
	DYNCLIENT = kubeconfig.DYNCLIENT
	CLIENTSET = kubeconfig.CLIENTSET
	STD_GVR   = kubeconfig.STD_GVR
	ADV_GVR   = kubeconfig.ADV_GVR
)
