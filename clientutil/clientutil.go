package clientutil

import (
	"context"
	"fmt"
	gsc "github.com/elankath/gardener-scaling-common"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func ListAllNodes(ctx context.Context, clientSet *kubernetes.Clientset) ([]corev1.Node, error) {
	return ListAllNodesWithPageSize(ctx, clientSet, 0)
}

func ListAllPods(ctx context.Context, clientSet *kubernetes.Clientset) ([]corev1.Pod, error) {
	return ListAllPodsWithPageSize(ctx, clientSet, 0)
}

func ListAllNodesWithPageSize(ctx context.Context, clientSet *kubernetes.Clientset, pageSize int) ([]corev1.Node, error) {
	// Initialize the list options with a page size
	var listOptions metav1.ListOptions
	if pageSize > 0 {
		listOptions = metav1.ListOptions{
			Limit: int64(pageSize), // Set a limit for pagination
		}
	}
	var allNodes []corev1.Node
	for {
		// List nodes with the current list options
		if ctx.Err() != nil {
			return nil, fmt.Errorf("cannot list nodes since context.Err is non-nil: %w", ctx.Err())
		}
		nodes, err := clientSet.CoreV1().Nodes().List(ctx, listOptions)
		if err != nil {
			return nil, err
		}
		// Append the current page of nodes to the allNodes slice
		allNodes = append(allNodes, nodes.Items...)
		// Check if there is another page
		if nodes.Continue == "" {
			break
		}
		// Set the continue token for the next request
		listOptions.Continue = nodes.Continue
	}
	return allNodes, nil
}

func ListAllPodsWithPageSize(ctx context.Context, clientSet *kubernetes.Clientset, pageSize int) ([]corev1.Pod, error) {
	// Initialize the list options with a page size
	var listOptions metav1.ListOptions
	if pageSize > 0 {
		listOptions = metav1.ListOptions{
			Limit: int64(pageSize), // Set a limit for pagination
		}
	}
	var allPods []corev1.Pod
	for {
		// List nodes with the current list options
		if ctx.Err() != nil {
			return nil, fmt.Errorf("cannot list Pods since context.Err is non-nil: %w", ctx.Err())
		}
		nodes, err := clientSet.CoreV1().Pods("").List(ctx, listOptions)
		if err != nil {
			return nil, err
		}
		// Append the current page of nodes to the allPods slice
		allPods = append(allPods, nodes.Items...)
		// Check if there is another page
		if nodes.Continue == "" {
			break
		}
		// Set the continue token for the next request
		listOptions.Continue = nodes.Continue
	}
	return allPods, nil
}

func GetKubeSystemPodsRequests(ctx context.Context, clientset *kubernetes.Clientset) (corev1.ResourceList, error) {
	podList, err := clientset.CoreV1().Pods("kube-system").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	podsByNode := lo.GroupBy(podList.Items, func(pod corev1.Pod) string {
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

	nodeResource := SumResourceRequest(podsByNode[nodeWithMostKubeSystemPods])

	return nodeResource, nil
}

func sumResourcesRequestsOld(pods []corev1.Pod) corev1.ResourceList {
	var totalMemory resource.Quantity
	var totalCPU resource.Quantity
	var storage resource.Quantity
	var ephemeralStorage resource.Quantity
	for _, pod := range pods {
		for _, container := range pod.Spec.Containers {
			totalMemory.Add(NilOr(container.Resources.Requests.Memory(), resource.Quantity{}))
			totalCPU.Add(NilOr(container.Resources.Requests.Cpu(), resource.Quantity{}))
			storage.Add(NilOr(container.Resources.Requests.Storage(), resource.Quantity{}))
			ephemeralStorage.Add(NilOr(container.Resources.Requests.Storage(), resource.Quantity{}))
		}
	}
	return corev1.ResourceList{
		corev1.ResourceMemory:           totalMemory,
		corev1.ResourceCPU:              totalCPU,
		corev1.ResourceStorage:          storage,
		corev1.ResourceEphemeralStorage: ephemeralStorage,
	}
}

func SumResourceRequest(pods []corev1.Pod) corev1.ResourceList {
	var allRequests []corev1.ResourceList

	for _, p := range pods {
		for _, container := range p.Spec.Containers {
			allRequests = append(allRequests, container.Resources.Requests)
		}
	}

	return gsc.SumResources(allRequests)
}

func NilOr[T any](val *T, defaultVal T) T {
	if val == nil {
		return defaultVal
	}
	return *val
}
