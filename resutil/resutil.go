package resutil

import (
	gsc "github.com/elankath/gardener-scaling-common"
	"github.com/elankath/gardener-scaling-common/clientutil"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func ComputeRevisedResources(original corev1.ResourceList, sysComponentMaxResourceList corev1.ResourceList) corev1.ResourceList {
	kubeReservedCPU := resource.MustParse("80m")
	kubeReservedMemory := resource.MustParse("1Gi")
	kubeReservedResources := corev1.ResourceList{corev1.ResourceCPU: kubeReservedCPU, corev1.ResourceMemory: kubeReservedMemory}
	return ComputeRevisedAllocatable(original, sysComponentMaxResourceList, kubeReservedResources)
}

func ComputeRevisedAllocatable(originalAllocatable corev1.ResourceList, systemComponentsResources corev1.ResourceList, kubeReservedResources corev1.ResourceList) corev1.ResourceList {
	revisedNodeAllocatable := originalAllocatable.DeepCopy()
	revisedMem := revisedNodeAllocatable.Memory()
	revisedMem.Sub(systemComponentsResources[corev1.ResourceMemory])

	revisedCPU := revisedNodeAllocatable.Cpu()
	revisedCPU.Sub(systemComponentsResources[corev1.ResourceCPU])

	if kubeReservedResources != nil {
		revisedMem.Sub(kubeReservedResources[corev1.ResourceMemory])
		revisedCPU.Sub(kubeReservedResources[corev1.ResourceCPU])
	}
	revisedNodeAllocatable[corev1.ResourceMemory] = *revisedMem
	revisedNodeAllocatable[corev1.ResourceCPU] = *revisedCPU
	return revisedNodeAllocatable
}

func ComputeKubeSystemResources(podInfos []gsc.PodInfo) corev1.ResourceList {
	ksPodInfos := lo.Filter(podInfos, func(item gsc.PodInfo, index int) bool {
		return item.Namespace == "kube-system"
	})

	podsByNode := lo.GroupBy(ksPodInfos, func(pod gsc.PodInfo) string {
		return pod.Spec.NodeName
	})

	nodeWithMostKubeSystemPods := ""
	numPods := 0
	for nodeName, nodePods := range podsByNode {
		if len(nodePods) > numPods {
			nodeWithMostKubeSystemPods = nodeName
			numPods = len(nodePods)
		}
	}

	podSpecs := lo.Map(podsByNode[nodeWithMostKubeSystemPods], func(item gsc.PodInfo, index int) corev1.PodSpec {
		return item.Spec
	})

	return clientutil.SumResourceRequest(podSpecs)
}
