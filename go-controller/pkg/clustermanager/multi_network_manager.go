package clustermanager

import (
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	controllerManager "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/network-controller-manager"
)

type multiNetworkClusterManager struct {
	ovnClient *util.OVNClientset
	recorder  record.EventRecorder

	// net-attach-def controller handle net-attach-def and create/delete network controllers
	nadController *controllerManager.NetAttachDefinitionController
}

func newMultiNetworkClusterManager(ovnClientset *util.OVNClientset,
	recorder record.EventRecorder) *multiNetworkClusterManager {
	multiNetManager := &multiNetworkClusterManager{
		ovnClient: ovnClientset,
		recorder:  recorder,
	}

	multiNetManager.nadController = controllerManager.NewNetAttachDefinitionController(
		multiNetManager, multiNetManager.ovnClient, multiNetManager.recorder)
	return multiNetManager
}

func (mm *multiNetworkClusterManager) Run(stopChan <-chan struct{}) error {
	klog.Infof("Starts net-attach-def controller")
	return mm.nadController.Run(stopChan)
}

func (mm *multiNetworkClusterManager) NewNetworkController(nInfo util.NetInfo,
	netConfInfo util.NetConfInfo) (controllerManager.NetworkController, error) {
	klog.Infof("New net-attach-def controller for network %s called", nInfo.GetNetworkName())
	return nil, nil
}

func (mm *multiNetworkClusterManager) CleanupDeletedNetworks(allControllers []controllerManager.NetworkController) error {
	return nil
}
