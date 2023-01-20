package clustermanager

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/clustermanager/subnetallocator"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	nad "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/network-attach-def-controller"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/retry"
	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	kapi "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

type multiNetworkClusterManager struct {
	ovnClient *util.OVNClientset
	recorder  record.EventRecorder
	// net-attach-def controller handle net-attach-def and create/delete network controllers
	nadController *nad.NetAttachDefinitionController
	cm            *ClusterManager
}

func newMultiNetworkClusterManager(ovnClient *util.OVNClientset,
	cm *ClusterManager) *multiNetworkClusterManager {
	klog.Infof("Creates new Multi Network cluster controller")
	multiNetManager := &multiNetworkClusterManager{
		ovnClient: ovnClient,
		recorder:  cm.recorder,
		cm:        cm,
	}
	multiNetManager.nadController = nad.NewNetAttachDefinitionController(
		multiNetManager, multiNetManager.ovnClient, multiNetManager.recorder)
	return multiNetManager
}

func (mm *multiNetworkClusterManager) Run(stopChan <-chan struct{}) error {
	klog.Infof("Starts net-attach-def controller")
	return mm.nadController.Run(stopChan)
}

// Start starts the secondary layer3 controller, handles all events and creates all needed logical entities
func (mm *multiNetworkClusterManager) Start(ctx context.Context) error {
	klog.Infof("Start secondary network controller of network")
	return nil
}

func (mm *multiNetworkClusterManager) Stop() {
	// then stops each network controller associated with net-attach-def; it is ok
	// to call GetAllControllers here as net-attach-def controller has been stopped,
	// and no more change of network controllers
	klog.Infof("Stops net-attach-def controller")
	for _, oc := range mm.nadController.GetAllNetworkControllers() {
		oc.Stop()
	}
}

type SecondaryNetworkController struct {
	ClusterManager
	// per controller NAD/netconf name information
	util.NetInfo
	util.NetConfInfo
	// retry framework for nodes
	retryNodes *retry.RetryFramework
	// node events factory handler
	nodeHandler *factory.Handler
	// waitGroup per-Controller
	wg       *sync.WaitGroup
	stopChan chan struct{}

	clusterSubnetAllocator *subnetallocator.HostSubnetAllocator

	// Node-specific syncMaps used by node event handler
	addNodeFailed sync.Map
}

// newSecondaryNetworkController create a new secondary controller for the given secondary layer3 NAD
func newSecondaryNetworkController(cm *ClusterManager, netInfo util.NetInfo, netconfInfo util.NetConfInfo) *SecondaryNetworkController {
	stopChan := make(chan struct{})
	oc := &SecondaryNetworkController{
		ClusterManager:         *cm,
		NetConfInfo:            netconfInfo,
		NetInfo:                netInfo,
		wg:                     &sync.WaitGroup{},
		stopChan:               stopChan,
		clusterSubnetAllocator: subnetallocator.NewHostSubnetAllocator(),
	}
	oc.initRetryFramework()
	layer3NetConfInfo := netconfInfo.(*util.Layer3NetConfInfo)
	if err := oc.clusterSubnetAllocator.InitRanges(layer3NetConfInfo.ClusterSubnets); err != nil {
		klog.Errorf("Failed to initialize host subnet allocator ranges: %v", err)
		return nil
	}
	return oc
}

type secondaryNetworkControllerEventHandler struct {
	watchFactory *factory.WatchFactory
	objType      reflect.Type
	oc           *SecondaryNetworkController
	syncFunc     func([]interface{}) error
}

func (oc *SecondaryNetworkController) initRetryFramework() {
	oc.retryNodes = oc.newRetryFramework(factory.NodeType)
}

// newRetryFramework builds and returns a retry framework for the input resource type;
func (oc *SecondaryNetworkController) newRetryFramework(
	objectType reflect.Type) *retry.RetryFramework {
	eventHandler := &secondaryNetworkControllerEventHandler{
		objType:      objectType,
		watchFactory: oc.watchFactory,
		oc:           oc,
		syncFunc:     nil,
	}
	resourceHandler := &retry.ResourceHandler{
		HasUpdateFunc:          true,
		NeedsUpdateDuringRetry: false,
		ObjType:                objectType,
		EventHandler:           eventHandler,
	}
	return retry.NewRetryFramework(
		oc.stopChan,
		oc.wg,
		oc.watchFactory,
		resourceHandler,
	)
}

// Start starts the secondary layer3 controller, handles all events and creates all needed logical entities
func (oc *SecondaryNetworkController) Start(ctx context.Context) error {
	klog.Infof("Start secondary %s network controller of network %s", oc.TopologyType(), oc.GetNetworkName())
	return oc.Run()
}

// Stop gracefully stops the controller, and delete all logical entities for this network if requested
func (oc *SecondaryNetworkController) Stop() {
	klog.Infof("Stop secondary %s network controller of network %s", oc.TopologyType(), oc.GetNetworkName())
	close(oc.stopChan)
	oc.wg.Wait()

	if oc.nodeHandler != nil {
		oc.watchFactory.RemoveNodeHandler(oc.nodeHandler)
	}
}

// Cleanup cleans up logical entities for the given network, called from net-attach-def routine
// could be called from a dummy Controller (only has CommonNetworkControllerInfo set)
func (oc *SecondaryNetworkController) Cleanup(netName string) error {
	var err error

	// remove hostsubnet annotation for this network
	klog.Infof("Remove node-subnets annotation for network %s on all nodes", netName)
	existingNodes, err := oc.watchFactory.GetNodes()
	if err != nil {
		klog.Errorf("Error in initializing/fetching subnets: %v", err)
		return nil
	}
	for _, node := range existingNodes {
		if util.NoHostSubnet(node) {
			klog.V(5).Infof("Node %s is not managed by OVN", node.Name)
			continue
		}
		hostSubnetsMap := map[string][]*net.IPNet{netName: nil}
		err = oc.updateNodeAnnotationWithRetry(node.Name, hostSubnetsMap)
		if err != nil {
			return fmt.Errorf("failed to clear node %q subnet annotation for network %s",
				node.Name, netName)
		}
	}
	return nil
}

func (oc *SecondaryNetworkController) Run() error {
	klog.Infof("Starting all the Watchers for network %s ...", oc.GetNetworkName())
	start := time.Now()

	if err := oc.WatchNodes(); err != nil {
		return err
	}

	klog.Infof("Completing all the Watchers for network %s took %v", oc.GetNetworkName(), time.Since(start))
	return nil
}

// WatchNodes starts the watching of node resource and calls
// back the appropriate handler logic
func (oc *SecondaryNetworkController) WatchNodes() error {
	if oc.nodeHandler != nil {
		return nil
	}
	handler, err := oc.retryNodes.WatchResource()
	if err == nil {
		oc.nodeHandler = handler
	}
	return err
}

func (oc *SecondaryNetworkController) addUpdateNodeEvent(node *kapi.Node, nSyncs *nodeSyncs) error {
	klog.Infof("Adding or Updating Node %q for network %s", node.Name, oc.GetNetworkName())
	if nSyncs.syncNode {
		if _, err := oc.addNode(node); err != nil {
			oc.addNodeFailed.Store(node.Name, true)
			err = fmt.Errorf("nodeAdd: error adding node %q for network %s: %w", node.Name, oc.GetNetworkName(), err)
			return err
		}
		oc.addNodeFailed.Delete(node.Name)
	}

	return nil
}

func (oc *SecondaryNetworkController) addNode(node *kapi.Node) ([]*net.IPNet, error) {
	existingSubnets, err := util.ParseNodeHostSubnetAnnotation(node, oc.GetNetworkName())
	if err != nil && !util.IsAnnotationNotSetError(err) {
		// Log the error and try to allocate new subnets
		klog.Infof("Failed to get node %s host subnets annotations: %v", node.Name, err)
		return nil, err
	}

	hostSubnets, allocatedSubnets, err :=
		oc.clusterSubnetAllocator.AllocateNodeSubnets(node.Name, existingSubnets, config.IPv4Mode, config.IPv6Mode)
	if err != nil {
		return nil, err
	}

	if len(allocatedSubnets) == 0 {
		return nil, nil
	}
	hostSubnetsMap := map[string][]*net.IPNet{oc.GetNetworkName(): hostSubnets}
	klog.Infof("Updating Node %q annotation for network %s hostSubnetMap %+v",
		node.Name, oc.GetNetworkName(), hostSubnetsMap)
	err = oc.updateNodeAnnotationWithRetry(node.Name, hostSubnetsMap)
	if err != nil {
		return nil, err
	}

	return hostSubnets, nil
}

func (oc *SecondaryNetworkController) deleteNodeEvent(node *kapi.Node) error {
	klog.V(5).Infof("Deleting Node %q for network %s. Removing the node from "+
		"various caches", node.Name, oc.GetNetworkName())

	if err := oc.deleteNode(node.Name); err != nil {
		return err
	}

	return nil
}

func (oc *SecondaryNetworkController) deleteNode(nodeName string) error {
	oc.clusterSubnetAllocator.ReleaseAllNodeSubnets(nodeName)
	return nil
}

type nodeSyncs struct {
	syncNode bool
}

func (h *secondaryNetworkControllerEventHandler) AreResourcesEqual(obj1, obj2 interface{}) (bool, error) {
	node1, ok := obj1.(*kapi.Node)
	if !ok {
		return false, fmt.Errorf("could not cast obj1 of type %T to *kapi.Node", obj1)
	}
	node2, ok := obj2.(*kapi.Node)
	if !ok {
		return false, fmt.Errorf("could not cast obj2 of type %T to *kapi.Node", obj2)
	}

	// when shouldUpdateNode is false, the hostsubnet is not assigned by ovn-kubernetes
	shouldUpdate, err := shouldUpdateNode(node2, node1)
	if err != nil {
		klog.Errorf(err.Error())
	}
	return !shouldUpdate, nil
}

// GetInternalCacheEntry returns the internal cache entry for this object, given an object and its type.
// This is now used only for pods, which will get their the logical port cache entry.
func (h *secondaryNetworkControllerEventHandler) GetInternalCacheEntry(obj interface{}) interface{} {
	return nil
}

// GetResourceFromInformerCache returns the latest state of the object, given an object key and its type.
// from the informers cache.
func (h *secondaryNetworkControllerEventHandler) GetResourceFromInformerCache(key string) (interface{}, error) {
	var obj interface{}
	var err error

	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to split key %s: %v", key, err)
	}

	if h.objType == factory.NodeType {
		obj, err = h.watchFactory.GetNode(name)
	}
	return obj, err
}

// RecordAddEvent records the add event on this given object.
func (h *secondaryNetworkControllerEventHandler) RecordAddEvent(obj interface{}) {
}

// RecordUpdateEvent records the udpate event on this given object.
func (h *secondaryNetworkControllerEventHandler) RecordUpdateEvent(obj interface{}) {
}

// RecordDeleteEvent records the delete event on this given object.
func (h *secondaryNetworkControllerEventHandler) RecordDeleteEvent(obj interface{}) {
}

// RecordSuccessEvent records the success event on this given object.
func (h *secondaryNetworkControllerEventHandler) RecordSuccessEvent(obj interface{}) {
}

// RecordErrorEvent records the error event on this given object.
func (h *secondaryNetworkControllerEventHandler) RecordErrorEvent(obj interface{}, reason string, err error) {
}

func (h *secondaryNetworkControllerEventHandler) IsResourceScheduled(obj interface{}) bool {
	return true
}

// IsObjectInTerminalState returns true if the object is in a terminal state.
func (h *secondaryNetworkControllerEventHandler) IsObjectInTerminalState(bj interface{}) bool {
	return false
}

// AddResource adds the specified object to the cluster according to its type and returns the error,
// if any, yielded during object creation.
// Given an object to add and a boolean specifying if the function was executed from iterateRetryResources
func (h *secondaryNetworkControllerEventHandler) AddResource(obj interface{}, fromRetryLoop bool) error {
	switch h.objType {
	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast %T object to *kapi.Node", obj)
		}
		var nodeParams *nodeSyncs
		if fromRetryLoop {
			_, nodeSync := h.oc.addNodeFailed.Load(node.Name)
			nodeParams = &nodeSyncs{syncNode: nodeSync}
		} else {
			nodeParams = &nodeSyncs{syncNode: true}
		}

		if err := h.oc.addUpdateNodeEvent(node, nodeParams); err != nil {
			klog.Errorf("Node add failed for %s, will try again later: %v",
				node.Name, err)
			return err
		}
	}
	return nil
}

// UpdateResource updates the specified object in the cluster to its version in newObj according to its
// type and returns the error, if any, yielded during the object update.
// Given an old and a new object; The inRetryCache boolean argument is to indicate if the given resource
// is in the retryCache or not.
func (h *secondaryNetworkControllerEventHandler) UpdateResource(oldObj, newObj interface{}, inRetryCache bool) error {
	switch h.objType {
	case factory.NodeType:
		newNode, ok := newObj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast newObj of type %T to *kapi.Node", newObj)
		}
		_, ok = oldObj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast oldObj of type %T to *kapi.Node", oldObj)
		}
		// determine what actually changed in this update
		_, nodeSync := h.oc.addNodeFailed.Load(newNode.Name)

		return h.oc.addUpdateNodeEvent(newNode, &nodeSyncs{syncNode: nodeSync})
	}
	return nil
}

// DeleteResource deletes the object from the cluster according to the delete logic of its resource type.
// Given an object and optionally a cachedObj; cachedObj is the internal cache entry for this object,
// used for now for pods and network policies.
func (h *secondaryNetworkControllerEventHandler) DeleteResource(obj, cachedObj interface{}) error {
	switch h.objType {
	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast obj of type %T to *knet.Node", obj)
		}
		return h.oc.deleteNodeEvent(node)

	}
	return nil
}

func (h *secondaryNetworkControllerEventHandler) SyncFunc(objs []interface{}) error {
	return nil
}

func (mm *multiNetworkClusterManager) NewNetworkController(nInfo util.NetInfo,
	netConfInfo util.NetConfInfo) (nad.NetworkController, error) {
	klog.Infof("New net-attach-def controller for network %s called", nInfo.GetNetworkName())
	cm := mm.cm
	topoType := netConfInfo.TopologyType()
	if topoType == ovntypes.Layer3Topology {
		klog.Infof("allocate subnet for %s", nInfo.GetNetworkName())
		oc := newSecondaryNetworkController(cm, nInfo, netConfInfo)
		return oc, nil
	}
	return nil, fmt.Errorf("topology type %s not supported", topoType)
}

func (mm *multiNetworkClusterManager) CleanupDeletedNetworks(allControllers []nad.NetworkController) error {
	// Nothing need to be done here
	return nil
}
