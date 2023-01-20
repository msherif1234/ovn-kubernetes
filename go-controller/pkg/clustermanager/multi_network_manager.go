package clustermanager

import (
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	nad "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/network-attach-def-controller"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

type multiNetworkClusterManager struct {
	ovnClient *util.OVNClientset
	recorder  record.EventRecorder

	// net-attach-def controller handle net-attach-def and create/delete network controllers
	nadController *nad.NetAttachDefinitionController
}

func newMultiNetworkClusterManager(ovnClient *util.OVNClientset,
	recorder record.EventRecorder) *multiNetworkClusterManager {
	multiNetManager := &multiNetworkClusterManager{
		ovnClient: ovnClient,
		recorder:  recorder,
	}

	multiNetManager.nadController = nad.NewNetAttachDefinitionController(
		multiNetManager, multiNetManager.ovnClient, multiNetManager.recorder)
	return multiNetManager
}

func (mm *multiNetworkClusterManager) Run(stopChan <-chan struct{}) error {
	klog.Infof("Starts net-attach-def controller")
	return mm.nadController.Run(stopChan)
}

func (mm *multiNetworkClusterManager) Stop() {
	// then stops each network controller associated with net-attach-def; it is ok
	// to call GetAllControllers here as net-attach-def controller has been stopped,
	// and no more change of network controllers
	for _, oc := range mm.nadController.GetAllNetworkControllers() {
		oc.Stop()
	}
}

func (mm *multiNetworkClusterManager) NewNetworkController(nInfo util.NetInfo,
	netConfInfo util.NetConfInfo) (nad.NetworkController, error) {
	klog.Infof("New net-attach-def controller for network %s called", nInfo.GetNetworkName())
	return nil, nil
}

func (mm *multiNetworkClusterManager) CleanupDeletedNetworks(allControllers []nad.NetworkController) error {
	return nil
}
