package gsc

import (
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"time"
)

const PoolLabel = "worker.gardener.cloud/pool"
const LabelVirtualScaled = "virtual-scaled"

var ZoneLabels = []string{"topology.gke.io/zone", "topology.ebs.csi.aws.com/zone"}

type AutoscalerMode string

const AutoscalerReplayerMode AutoscalerMode = "replay-mode"

const AutoscalerStandaloneMode AutoscalerMode = "standalone-mode"

type AutoscalerConfig struct {
	NodeTemplates     map[string]NodeTemplate
	NodeGroups        map[string]NodeGroupInfo
	ExistingNodes     []NodeInfo
	CASettings        CASettingsInfo
	Mode              AutoscalerMode
	SuccessSignalPath string
	ErrorSignalPath   string
	Hash              string
}

type NodeGroupInfo struct {
	Name       string
	PoolName   string
	Zone       string
	TargetSize int
	MinSize    int
	MaxSize    int
	Hash       string
}

// WorkerPoolInfo represents snapshot information corresponding to the gardener shoot worker pool.
type WorkerPoolInfo struct {
	SnapshotMeta
	MachineType       string
	Architecture      string
	Minimum           int
	Maximum           int
	MaxSurge          intstr.IntOrString
	MaxUnavailable    intstr.IntOrString
	Zones             []string
	DeletionTimestamp time.Time
	Hash              string
}

type NodeTemplate struct {
	Name string
	//CPU              resource.Quantity
	//GPU              resource.Quantity
	//Memory           resource.Quantity
	//EphemeralStorage resource.Quantity
	InstanceType string
	Region       string
	Zone         string
	Capacity     corev1.ResourceList
	Allocatable  corev1.ResourceList
	Labels       map[string]string
	Taints       []corev1.Taint
	Hash         string
}

type SnapshotMeta struct {
	RowID             int64
	CreationTimestamp time.Time
	SnapshotTimestamp time.Time
	Name              string
	Namespace         string
}

// MachineDeploymentInfo represents snapshot information captured about the MCM MachineDeployment object
// present in the control plane of a gardener shoot cluster.
type MachineDeploymentInfo struct {
	SnapshotMeta
	Replicas          int
	PoolName          string
	Zone              string
	MaxSurge          intstr.IntOrString
	MaxUnavailable    intstr.IntOrString
	MachineClassName  string
	DeletionTimestamp time.Time
	Labels            map[string]string
	Taints            []corev1.Taint
	Hash              string
}

type MinMax struct {
	Min int
	Max int
}

// CASettingsInfo represents configuration settings of the k8s cluster-autoscaler.
// This is currently a very minimal struct only capturing those options that
// can be configured in a gardener shoot spec.
// TODO Also add scale down properties
type CASettingsInfo struct {
	SnapshotTimestamp             time.Time
	Expander                      string
	NodeGroupsMinMax              map[string]MinMax
	MaxNodeProvisionTime          time.Duration
	ScanInterval                  time.Duration
	MaxGracefulTerminationSeconds int
	NewPodScaleUpDelay            time.Duration
	MaxEmptyBulkDelete            int
	IgnoreDaemonSetUtilization    bool
	MaxNodesTotal                 int `db:"MaxNodesTotal"`
	// Priorities is the value of the `priorities` key in the `cluster-autoscaler-priority-expander` config map.
	// See https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/expander/priority/readme.md#configuration
	Priorities string
	Hash       string //primary key
}

type PodScheduleStatus int

const PodSchedulePending = -2
const PodScheduleNominated = -1
const PodUnscheduled = 0
const PodScheduleCommited = 1

// PodInfo represents snapshot information captured about a k8s Pod deployed into
// the cluster at a particular moment in time. When the `Pod` is deleted its `DeletionTimestamp` is updated.
type PodInfo struct {
	SnapshotMeta
	UID               string
	NodeName          string
	NominatedNodeName string
	Labels            map[string]string
	Requests          corev1.ResourceList
	Spec              corev1.PodSpec
	PodScheduleStatus PodScheduleStatus
	DeletionTimestamp time.Time
	Hash              string
}

// NodeInfo represents snapshot information captured about an active k8s Node in the cluster at a particular moment in time.
// . A NodeInfo snapshot is only captured if there is a change in the properties excepting for DeletionTimestamp, in
// which case the DeletionTimestamp is only updated.
type NodeInfo struct {
	SnapshotMeta
	ProviderID         string
	AllocatableVolumes int
	Labels             map[string]string
	Taints             []corev1.Taint
	Allocatable        corev1.ResourceList
	Capacity           corev1.ResourceList
	DeletionTimestamp  time.Time
	Hash               string
}

type PriorityClassInfo struct {
	RowID             int64
	SnapshotTimestamp time.Time
	Hash              string
	schedulingv1.PriorityClass
}

// EventInfo represents information about an event emitted in the k8s cluster.
type EventInfo struct {
	UID                     string    `db:"UID"`
	EventTime               time.Time `db:"EventTime"`
	ReportingController     string    `db:"ReportingController"`
	Reason                  string    `db:"Reason"`
	Message                 string    `db:"Message"`
	InvolvedObjectKind      string    `db:"InvolvedObjectKind"`
	InvolvedObjectName      string    `db:"InvolvedObjectName"`
	InvolvedObjectNamespace string    `db:"InvolvedObjectNamespace"`
	InvolvedObjectUID       string    `db:"InvolvedObjectUID"`
}

// ClusterSnapshot represents captured snapshot information about a gardener cluster that is useful for auto-scaling state.
type ClusterSnapshot struct {
	ID               string
	Number           int
	SnapshotTime     time.Time
	AutoscalerConfig AutoscalerConfig
	WorkerPools      []WorkerPoolInfo
	PriorityClasses  []PriorityClassInfo
	Pods             []PodInfo
	Nodes            []NodeInfo
	Hash             string
}
