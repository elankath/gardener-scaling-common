package resutil

import (
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
