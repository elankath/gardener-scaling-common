package clientutil

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
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
