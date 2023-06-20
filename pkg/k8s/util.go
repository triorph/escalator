package k8s

import (
	k8s_resource "github.com/atlassian/escalator/pkg/k8s/resource"
	"github.com/atlassian/escalator/pkg/k8s/scheduler"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// PodIsDaemonSet returns if the pod is a daemonset or not
func PodIsDaemonSet(pod *v1.Pod) bool {
	for _, ownerReference := range pod.ObjectMeta.OwnerReferences {
		if ownerReference.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

// PodIsStatic returns if the pod is static or not
func PodIsStatic(pod *v1.Pod) bool {
	configSource, ok := pod.ObjectMeta.Annotations["kubernetes.io/config.source"]
	return ok && configSource == "file"
}

type PodRequestedUsage struct {
	Total         ResourceItem
	LargestMemory ResourceItem
	LargestCPU    ResourceItem
}

type NodeAvailableCapacity struct {
	Total                  ResourceItem
	LargestAvailableMemory ResourceItem
	LargestAvailableCPU    ResourceItem
}

type ResourceItem struct {
	CPU    resource.Quantity
	Memory resource.Quantity
}

func newEmptyResourceItem() ResourceItem {
	return ResourceItem{
		CPU:    *k8s_resource.NewCPUQuantity(0),
		Memory: *k8s_resource.NewMemoryQuantity(0),
	}
}

func newResourceItem(cpu int64, memory int64) ResourceItem {
	return ResourceItem{
		CPU:    *k8s_resource.NewCPUQuantity(cpu),
		Memory: *k8s_resource.NewMemoryQuantity(memory),
	}
}

func NewPodRequestedUsage() PodRequestedUsage {
	return PodRequestedUsage{
		Total:         newEmptyResourceItem(),
		LargestMemory: newEmptyResourceItem(),
		LargestCPU:    newEmptyResourceItem(),
	}
}

func newNodeAvailableCapacity() NodeAvailableCapacity {
	return NodeAvailableCapacity{
		Total:                  newEmptyResourceItem(),
		LargestAvailableMemory: newEmptyResourceItem(),
		LargestAvailableCPU:    newEmptyResourceItem(),
	}
}

// CalculatePodsRequestedUsage returns the total capacity of all pods
func CalculatePodsRequestedUsage(pods []*v1.Pod) (PodRequestedUsage, error) {
	ret := NewPodRequestedUsage()

	for _, pod := range pods {
		podResources := scheduler.ComputePodResourceRequest(pod)
		ret.Total.Memory.Add(*k8s_resource.NewMemoryQuantity(podResources.Memory))
		ret.Total.CPU.Add(*k8s_resource.NewCPUQuantity(podResources.MilliCPU))
		if pod.Status.Phase == v1.PodPending {
			if podResources.Memory > ret.LargestMemory.Memory.Value() {
				ret.LargestMemory = newResourceItem(podResources.MilliCPU, podResources.Memory)
			}
			if podResources.MilliCPU > ret.LargestCPU.CPU.MilliValue() {
				ret.LargestCPU = newResourceItem(podResources.MilliCPU, podResources.Memory)
			}
		}
	}

	return ret, nil
}

// CalculateNodesCapacity calculates the total Allocatable node capacity for all nodes
func CalculateNodesCapacity(nodes []*v1.Node, pods []*v1.Pod) (NodeAvailableCapacity, error) {
	ret := newNodeAvailableCapacity()

	mappedPods := mapPods(pods)
	for _, node := range nodes {
		ret.Total.Memory.Add(*node.Status.Allocatable.Memory())
		ret.Total.CPU.Add(*node.Status.Allocatable.Cpu())
		availableResource := GetNodeAvailableResources(node, mappedPods)
		if availableResource.CPU.MilliValue() > ret.LargestAvailableCPU.CPU.MilliValue() {
			ret.LargestAvailableCPU = ResourceItem{
				CPU:    *node.Status.Allocatable.Cpu(),
				Memory: *node.Status.Allocatable.Memory(),
			}
		}
		if availableResource.Memory.Value() > ret.LargestAvailableMemory.Memory.Value() {
			ret.LargestAvailableMemory = ResourceItem{
				CPU:    *node.Status.Allocatable.Cpu(),
				Memory: *node.Status.Allocatable.Memory(),
			}
		}
	}

	return ret, nil
}

func mapPods(pods []*v1.Pod) map[string]([]*v1.Pod) {
	ret := make(map[string]([]*v1.Pod))
	for _, pod := range pods {
		name := pod.Status.NominatedNodeName
		val, found := ret[name]
		if !found {
			ret[name] = make([]*v1.Pod, 0)
			val = ret[name]
		}
		ret[name] = append(val, pod)
	}
	return ret
}

func sumByFunc(pods []*v1.Pod, f func(*v1.Pod) int64) int64 {
	ret := int64(0)
	for _, pod := range pods {
		if isPodUsingNodeResources(pod) {
			ret += f(pod)
		}
	}
	return ret
}

func isPodScheduled(pod *v1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == v1.PodScheduled {
			return condition.Status == v1.ConditionTrue
		}
	}
	return false
}

func isPodUsingNodeResources(pod *v1.Pod) bool {
	return isPodScheduled(pod) &&
		(pod.Status.Phase == v1.PodPending || pod.Status.Phase == v1.PodRunning)
}

func GetNodeAvailableResources(node *v1.Node, pods map[string]([]*v1.Pod)) ResourceItem {
	filteredPods := pods[node.Name] // We are not 100% that this maps to pod.Status.NominatedNodeName so reviewer expertise would be appreciated here
	usedCpu := sumByFunc(filteredPods, func(pod *v1.Pod) int64 {
		podResources := scheduler.ComputePodResourceRequest(pod)
		return podResources.MilliCPU
	})
	usedMemory := sumByFunc(filteredPods, func(pod *v1.Pod) int64 {
		podResources := scheduler.ComputePodResourceRequest(pod)
		return podResources.Memory
	})
	return newResourceItem(node.Status.Allocatable.Cpu().MilliValue()-usedCpu,
		node.Status.Allocatable.Memory().Value()-usedMemory)

}
