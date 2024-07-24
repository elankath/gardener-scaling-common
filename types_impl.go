package gsc

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/samber/lo"
	"golang.org/x/exp/maps"
	"hash"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"slices"
	"strings"
	"time"
)

func (m MinMax) String() string {
	return fmt.Sprintf("(%d,%d)", m.Min, m.Max)
}

func (w WorkerPoolInfo) String() string {
	metaStr := header("WorkerPoolInfo", w.SnapshotMeta)
	return fmt.Sprintf("%s, MachineType=%d, Architecture=%s, Minimum=%d, Maximum=%d, MaxSurge=%s, MaxUnavailable=%s, Zones=%s, Hash=%s)",
		metaStr, w.MachineType, w.Architecture, w.Minimum, w.Maximum, w.MaxSurge.String(), w.MaxUnavailable.String(), w.Zones, w.Hash)
}

func (w WorkerPoolInfo) GetHash() string {
	hasher := md5.New()
	hasher.Write([]byte(w.Name))
	int64buf := make([]byte, 8) // 8 bytes for int64

	binary.BigEndian.PutUint64(int64buf, uint64(w.CreationTimestamp.UnixMilli()))
	hasher.Write(int64buf)

	hasher.Write([]byte(w.MachineType))
	hasher.Write([]byte(w.Architecture))

	binary.BigEndian.PutUint64(int64buf, uint64(w.Minimum))
	hasher.Write(int64buf)

	binary.BigEndian.PutUint64(int64buf, uint64(w.Maximum))
	hasher.Write(int64buf)

	hasher.Write([]byte(w.MaxSurge.String()))
	hasher.Write([]byte(w.MaxUnavailable.String()))

	HashSlice(hasher, w.Zones)

	return hex.EncodeToString(hasher.Sum(nil))
}

func (ng NodeGroupInfo) String() string {
	return fmt.Sprintf("NodeGroupInfo(Name: %s, PoolName: %s, Zone: %s, TargetSize: %d, MinSize: %d, MaxSize: %d)",
		ng.Name, ng.PoolName, ng.Zone, ng.TargetSize, ng.MinSize, ng.MaxSize)
}

func (ng NodeGroupInfo) GetHash() string {
	hasher := md5.New()
	hasher.Write([]byte(ng.Name))
	int64buf := make([]byte, 8) // 8 bytes for int64

	binary.BigEndian.PutUint64(int64buf, uint64(ng.TargetSize))
	hasher.Write(int64buf)

	binary.BigEndian.PutUint64(int64buf, uint64(ng.MinSize))
	hasher.Write(int64buf)

	binary.BigEndian.PutUint64(int64buf, uint64(ng.MaxSize))
	hasher.Write(int64buf)

	hasher.Write([]byte(ng.Zone))
	hasher.Write([]byte(ng.PoolName))

	return hex.EncodeToString(hasher.Sum(nil))
}

func (a AutoscalerConfig) GetHash() string {
	hasher := md5.New()
	keys := maps.Keys(a.NodeTemplates)
	//TODO optimize to a generic method
	slices.Sort(keys)
	for _, key := range keys {
		hasher.Write([]byte(key))
		hasher.Write([]byte(a.NodeTemplates[key].Hash))
	}
	keys = maps.Keys(a.NodeGroups)
	slices.Sort(keys)
	for _, key := range keys {
		hasher.Write([]byte(key))
		hasher.Write([]byte(a.NodeGroups[key].Hash))
	}
	slices.SortFunc(a.ExistingNodes, func(a, b NodeInfo) int {
		return strings.Compare(a.Name, b.Name)
	})
	for _, node := range a.ExistingNodes {
		node.Hash = node.GetHash()
		hasher.Write([]byte(node.Hash))
	}
	hasher.Write([]byte(a.CASettings.Hash))
	return hex.EncodeToString(hasher.Sum(nil))
}

func (t NodeTemplate) GetHash() string {
	hasher := md5.New()
	hasher.Write([]byte(t.Name))
	hasher.Write([]byte(t.InstanceType))
	hasher.Write([]byte(t.Region))
	hasher.Write([]byte(t.Zone))
	HashResources(hasher, t.Capacity)
	HashLabels(hasher, t.Labels)
	HashTaints(hasher, t.Taints)
	return hex.EncodeToString(hasher.Sum(nil))
}

func header(prefix string, meta SnapshotMeta) string {
	return fmt.Sprintf("%s(RowID=%d, CreationTimestamp=%s, SnapshotTimestamp=%s, Name=%s, Namespace=%s",
		prefix, meta.RowID, meta.CreationTimestamp, meta.SnapshotTimestamp, meta.Name, meta.Namespace)
}
func (m MachineDeploymentInfo) String() string {
	metaStr := header("MachineDeployment", m.SnapshotMeta)
	return fmt.Sprintf("%s, Replicas=%d, PoolName=%s, Zone=%s, MaxSurge=%s, MaxUnavailable=%s, MachineClassName=%s, Labels=%s, Hash=%s)",
		metaStr, m.Replicas, m.PoolName, m.Zone, m.MaxSurge.String(), m.MaxUnavailable.String(), m.MachineClassName, m.Labels, m.Hash)
}

func (m MachineDeploymentInfo) GetHash() string {
	int64buf := make([]byte, 8) // 8 bytes for int64

	hasher := md5.New()
	hasher.Write([]byte(m.Name))
	hasher.Write([]byte(m.Namespace))

	hasher.Write([]byte(m.PoolName))
	hasher.Write([]byte(m.Zone))

	binary.BigEndian.PutUint64(int64buf, uint64(m.Replicas))
	hasher.Write(int64buf)

	hasher.Write([]byte(m.MaxSurge.String()))
	hasher.Write([]byte(m.MaxUnavailable.String()))
	hasher.Write([]byte(m.MachineClassName))
	HashTaints(hasher, m.Taints)
	return hex.EncodeToString(hasher.Sum(nil))
}

func (n NodeInfo) String() string {
	return fmt.Sprintf(
		"NodeInfo(Name=%s, Namespace=%s, CreationTimestamp=%s, ProviderID=%s, Labels=%s, Taints=%s, Allocatable=%s, Capacity=%s)",
		n.Name, n.Namespace, n.CreationTimestamp, n.ProviderID, n.Labels, n.Taints, ResourcesAsString(n.Allocatable), ResourcesAsString(n.Capacity))
}

func (n NodeInfo) GetHash() string {
	hasher := md5.New()
	hasher.Write([]byte(n.Name))
	hasher.Write([]byte(n.Namespace))
	HashLabels(hasher, n.Labels)
	for _, t := range n.Taints {
		hasher.Write([]byte(t.Key))
		hasher.Write([]byte(t.Value))
		hasher.Write([]byte(t.Effect))
	}
	HashResources(hasher, n.Allocatable)
	HashResources(hasher, n.Capacity)
	return hex.EncodeToString(hasher.Sum(nil))
}

func CmpNodeInfoDescending(a, b NodeInfo) int {
	return b.CreationTimestamp.Compare(a.CreationTimestamp)
}

func IsEqualNodeInfo(a, b NodeInfo) bool {
	return a.Name == b.Name && a.Namespace == b.Namespace &&
		a.AllocatableVolumes == b.AllocatableVolumes &&
		maps.Equal(a.Labels, b.Labels) && slices.EqualFunc(a.Taints, b.Taints, IsEqualTaint) &&
		maps.Equal(a.Allocatable, b.Allocatable) && maps.Equal(a.Capacity, b.Capacity) &&
		a.Hash == b.Hash
}

func IsEqualQuantity(a, b resource.Quantity) bool {
	return a.Equal(b)
}

func IsEqualPodInfo(a, b PodInfo) bool {
	return a.GetHash() == b.GetHash()
}

func IsEqualTaint(a, b corev1.Taint) bool {
	return a.Key == b.Key && a.Value == b.Value && a.Effect == b.Effect
}

func (p PodInfo) String() string {
	metaStr := header("PodInfo", p.SnapshotMeta)
	return fmt.Sprintf("%s, UID=%s, NodeName=%s, NominatedNodeName=%s, Labels=%s, Requests=%s, Hash=%s)",
		metaStr, p.UID, p.NodeName, p.NominatedNodeName, p.Labels, ResourcesAsString(p.Requests), p.Hash)
}

func (p PodInfo) GetHash() string {
	hasher := md5.New()
	hasher.Write([]byte(p.Name))
	hasher.Write([]byte(p.Namespace))
	hasher.Write([]byte(p.NodeName))
	hasher.Write([]byte(p.NominatedNodeName))
	HashLabels(hasher, p.Labels)
	hasher.Write([]byte(p.Spec.SchedulerName))
	int64buf := make([]byte, 8) // 8 bytes for int64
	binary.BigEndian.PutUint64(int64buf, uint64(p.PodScheduleStatus))
	hasher.Write(int64buf)

	//binary.BigEndian.PutUint64(int64buf, uint64(p.CreationTimestamp.UnixMilli()))
	//hasher.Write(int64buf)

	slices.SortFunc(p.Spec.Containers, func(a, b corev1.Container) int {
		return strings.Compare(a.Name, b.Name)
	})
	for _, c := range p.Spec.Containers {
		hasher.Write([]byte(c.Name))
		HashSlice(hasher, c.Args)
		HashSlice(hasher, c.Command)
		hasher.Write([]byte(c.Image))
		for _, e := range c.Env {
			hasher.Write([]byte(e.Name))
			hasher.Write([]byte(e.Value))
		}
	}
	HashResources(hasher, p.Requests)
	slices.SortFunc(p.Spec.Tolerations, func(a, b corev1.Toleration) int {
		return strings.Compare(a.Key, b.Key)
	})
	for _, t := range p.Spec.Tolerations {
		hasher.Write([]byte(t.Key))
		hasher.Write([]byte(t.Operator))
		hasher.Write([]byte(t.Value))
		hasher.Write([]byte(t.Effect))
		//TODO: TolerationSeconds to Hash ??
	}
	slices.SortFunc(p.Spec.TopologySpreadConstraints, func(a, b corev1.TopologySpreadConstraint) int {
		return strings.Compare(a.TopologyKey, b.TopologyKey)
	})
	for _, tsc := range p.Spec.TopologySpreadConstraints {
		binary.BigEndian.PutUint64(int64buf, uint64(tsc.MaxSkew))
		hasher.Write(int64buf)

		hasher.Write([]byte(tsc.TopologyKey))
		hasher.Write([]byte(tsc.WhenUnsatisfiable))
		if tsc.LabelSelector != nil {
			hasher.Write([]byte(tsc.LabelSelector.String()))
		}
		if tsc.MinDomains != nil {
			binary.BigEndian.PutUint64(int64buf, uint64(*tsc.MinDomains))
			hasher.Write(int64buf)
		}
		if tsc.NodeAffinityPolicy != nil {
			hasher.Write([]byte(*tsc.NodeAffinityPolicy))
		}
		if tsc.NodeTaintsPolicy != nil {
			hasher.Write([]byte(*tsc.NodeTaintsPolicy))
		}
		for _, lk := range tsc.MatchLabelKeys {
			hasher.Write([]byte(lk))
		}
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func (p PriorityClassInfo) String() string {
	return fmt.Sprintf("PriorityClassInfo(RowID=%d,  CreationTimestamp=%s, SnapshotTimestamp=%s, Name=%s, Value=%d, PreemptionPolicy=%s, GlobalDefault=%t)",
		p.RowID, p.CreationTimestamp, p.SnapshotTimestamp, p.Name, p.Value, *p.PreemptionPolicy, p.GlobalDefault)
}

func (p PriorityClassInfo) GetHash() string {
	int64buf := make([]byte, 8) // 8 bytes for int64

	hasher := md5.New()
	hasher.Write([]byte(p.Name))

	binary.BigEndian.PutUint64(int64buf, uint64(p.CreationTimestamp.UTC().UnixMilli()))
	hasher.Write(int64buf)

	binary.BigEndian.PutUint32(int64buf, uint32(p.Value))
	hasher.Write(int64buf)

	hasher.Write(BoolToBytes(p.GlobalDefault))

	if p.PreemptionPolicy != nil {
		hasher.Write([]byte(*p.PreemptionPolicy))
	}

	return hex.EncodeToString(hasher.Sum(nil))
}

func ContainsPod(podUID string, podInfos []PodInfo) bool {
	return slices.ContainsFunc(podInfos, func(info PodInfo) bool {
		return info.UID == podUID
	})
}

func HashResources(hasher hash.Hash, resources corev1.ResourceList) {
	keys := maps.Keys(resources)
	slices.Sort(keys)
	for _, k := range keys {
		HashResource(hasher, k, resources[k])
	}
}
func HashSlice[T ~string](hasher hash.Hash, strSlice []T) {
	for _, t := range strSlice {
		hasher.Write([]byte(t))
	}
}

func HashLabels(hasher hash.Hash, labels map[string]string) {
	keys := maps.Keys(labels)
	slices.Sort(keys)
	for _, k := range keys {
		hasher.Write([]byte(k))
		hasher.Write([]byte(labels[k]))
	}
}

func HashBool(hasher hash.Hash, val bool) {
	var v byte
	if val {
		v = 0
	} else {
		v = 1
	}
	hasher.Write([]byte{v})
}
func HashInt(hasher hash.Hash, val int) {
	int64buf := make([]byte, 8) // 8 bytes for int64
	binary.BigEndian.PutUint64(int64buf, uint64(val))
	hasher.Write(int64buf)
}

func HashInt64(hasher hash.Hash, val int64) {
	int64buf := make([]byte, 8) // 8 bytes for int64
	binary.BigEndian.PutUint64(int64buf, uint64(val))
	hasher.Write(int64buf)
}

func HashDuration(hasher hash.Hash, d time.Duration) {
	HashInt64(hasher, d.Milliseconds())
}

func HashResource(hasher hash.Hash, name corev1.ResourceName, quantity resource.Quantity) {
	hasher.Write([]byte(name))
	rvBytes, _ := quantity.AsCanonicalBytes(nil)
	hasher.Write(rvBytes)
}

func HashTaints(hasher hash.Hash, taints []corev1.Taint) {
	for _, t := range taints {
		hasher.Write([]byte(t.Key))
		hasher.Write([]byte(t.Value))
		hasher.Write([]byte(t.Effect))
	}
}

func ResourcesAsString(resources corev1.ResourceList) string {
	if len(resources) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("(")
	var j int
	for k, v := range resources {
		sb.WriteString(string(k))
		sb.WriteString(":")
		sb.WriteString(v.String())
		j++
		if j != len(resources) {
			sb.WriteString(",")
		}
	}
	sb.WriteString(")")
	return sb.String()
}

func (eI EventInfo) String() string {
	return fmt.Sprintf("EventInfo : (UID = %s,EventTime = %s, ReportingController = %s, Reason = %s, Message = %s, InvolvedObjectName = %s,InvolvedObjectNamespace = %s, InvolvedObjectUID = %s)",
		eI.UID, eI.EventTime, eI.ReportingController, eI.Reason, eI.Message, eI.InvolvedObjectName, eI.InvolvedObjectNamespace, eI.InvolvedObjectUID)
}

func IsResourceListEqual(r1 corev1.ResourceList, r2 corev1.ResourceList) bool {
	return maps.EqualFunc(r1, r2, func(q1 resource.Quantity, q2 resource.Quantity) bool {
		return q1.Equal(q2)
	})
}

func (cas CASettingsInfo) GetHash() string {
	hasher := md5.New()
	hasher.Write([]byte(cas.Expander))

	keys := maps.Keys(cas.NodeGroupsMinMax)
	slices.Sort(keys)
	for _, k := range keys {
		mm := cas.NodeGroupsMinMax[k]
		HashInt(hasher, mm.Min)
		HashInt(hasher, mm.Max)
	}

	HashDuration(hasher, cas.MaxNodeProvisionTime)
	HashDuration(hasher, cas.ScanInterval)
	HashInt(hasher, cas.MaxGracefulTerminationSeconds)
	HashDuration(hasher, cas.NewPodScaleUpDelay)
	HashInt(hasher, cas.MaxEmptyBulkDelete)
	HashBool(hasher, cas.IgnoreDaemonSetUtilization)
	HashInt(hasher, cas.MaxNodesTotal)
	hasher.Write([]byte(cas.Priorities))
	return hex.EncodeToString(hasher.Sum(nil))
}

func SumResources(resources []corev1.ResourceList) corev1.ResourceList {
	sumResources := make(corev1.ResourceList)
	for _, r := range resources {
		for name, quantity := range r {
			sumQuantity, ok := sumResources[name]
			if ok {
				sumQuantity.Add(quantity)
				sumResources[name] = sumQuantity
			} else {
				sumResources[name] = quantity
			}
		}
	}
	//memory, ok := sumResources[corev1.ResourceMemory]
	//if ok && memory.Format == resource.DecimalSI {
	//	absVal, ok := memory.AsInt64()
	//	if !ok {
	//		return nil, fmt.Errorf("cannot get  the absolute value for memory quantity")
	//	}
	//	binaryMem, err := resource.ParseQuantity(fmt.Sprintf("%dKi", absVal/1024))
	//	if err != nil {
	//		return nil, fmt.Errorf("cannot parse the memory quantity: %w", err)
	//	}
	//	sumResources[corev1.ResourceMemory] = binaryMem
	//}
	return sumResources
}

func CumulatePodRequests(pod *corev1.Pod) corev1.ResourceList {
	sumRequests := make(corev1.ResourceList)
	for _, container := range pod.Spec.Containers {
		for name, quantity := range container.Resources.Requests {
			sumQuantity, ok := sumRequests[name]
			if ok {
				sumQuantity.Add(quantity)
				sumRequests[name] = sumQuantity
			} else {
				sumRequests[name] = quantity
			}
		}
	}
	return sumRequests
}

var ErrKeyNotFound = errors.New("key not found")

func GetZone(labelsMap map[string]any) string {
	var zone string
	for _, zoneLabel := range ZoneLabels {
		z, ok := labelsMap[zoneLabel]
		if ok {
			zone = z.(string)
			break
		}
	}
	return zone
}

func GetInnerMap(parentMap map[string]any, keys ...string) (map[string]any, error) {
	var mapPath []string
	childMap := parentMap
	for _, k := range keys {
		mapPath = append(mapPath, k)
		mp, ok := childMap[k]
		if !ok {
			return nil, fmt.Errorf("cannot find the child map under mapPath: %s", mapPath)
		}
		childMap, ok = mp.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("child map is not of type map[string] any under the mapPath: %s", mapPath)
		}
	}
	return childMap, nil
}

func GetInnerMapValue(parentMap map[string]any, keys ...string) (any, error) {
	subkeys := keys[:len(keys)-1]
	childMap, err := GetInnerMap(parentMap, subkeys...)
	if err != nil {
		return nil, err
	}
	val, ok := childMap[keys[len(keys)-1]]
	if !ok {
		return nil, fmt.Errorf("could not find value for keys %q : %w", keys, ErrKeyNotFound)
	}
	return val, nil
}

func AsIntOrString(val any) (target intstr.IntOrString, err error) {
	switch v := val.(type) {
	case int64:
		target = intstr.FromInt32(int32(val.(int64)))
	case int32:
		target = intstr.FromInt32(v)
	case string:
		return intstr.FromString(v), nil
	default:
		err = fmt.Errorf("cannot parse value %q as intstr.IntOrString", val)
	}
	return
}

func (c *AutoscalerConfig) Init() error {
	for name, minMax := range c.CASettings.NodeGroupsMinMax {
		nodeGroup, ok := c.NodeGroups[name]
		if !ok {
			return fmt.Errorf("no nodegroup with the name %s", name)
		}
		nodeGroup.MinSize = minMax.Min
		nodeGroup.MaxSize = minMax.Max
		c.NodeGroups[name] = nodeGroup
	}
	return nil
}

// MustParseQuantity parses given str as normalized quantity or panics.
// NOTE: ONLY USE FOR UNIT TESTS OR LITERALS
func MustParseQuantity(str string) (norm resource.Quantity) {
	q := resource.MustParse(str)
	norm, err := NormalizeQuantity(q)
	if err != nil {
		panic(err)
	}
	return
}

func AsQuantity(str string) (norm resource.Quantity, err error) {
	q, err := resource.ParseQuantity(str)
	if err != nil {
		return
	}
	return NormalizeQuantity(q)
}

func NormalizeQuantity(q resource.Quantity) (norm resource.Quantity, err error) {
	qstr := q.String()
	norm, err = resource.ParseQuantity(qstr)
	return
}

func BoolToBytes(b bool) []byte {
	var byteVal byte
	if b {
		byteVal = 1
	} else {
		byteVal = 0
	}
	return []byte{byteVal}
}

func (c ClusterSnapshot) GetPodUIDs() sets.Set[string] {
	uids := lo.Map(c.Pods, func(item PodInfo, index int) string {
		return item.UID
	})
	return sets.New(uids...)
}

func (c ClusterSnapshot) GetPodsWithScheduleStatus(status PodScheduleStatus) []PodInfo {
	return lo.Filter(c.Pods, func(item PodInfo, _ int) bool {
		return item.PodScheduleStatus == status
	})
}

func (c ClusterSnapshot) GetPodNamspaces() sets.Set[string] {
	namespaces := lo.Map(c.Pods, func(item PodInfo, index int) string {
		return item.Namespace
	})
	return sets.New(namespaces...)
}

func (c ClusterSnapshot) GetPriorityClassUIDs() sets.Set[string] {
	uids := lo.Map(c.PriorityClasses, func(item PriorityClassInfo, index int) string {
		return string(item.UID)
	})
	return sets.New(uids...)
}

func (c ClusterSnapshot) HasSameUnscheduledPods(other ClusterSnapshot) bool {
	pods1 := c.GetPodsWithScheduleStatus(PodUnscheduled)
	pods2 := other.GetPodsWithScheduleStatus(PodUnscheduled)
	// assumes that pods1 and pods2 are sorted according to same order.
	return slices.EqualFunc(pods1, pods2, func(p PodInfo, q PodInfo) bool {
		return p.UID == q.UID
	})
}

func CompareEventsByEventTime(a, b EventInfo) int {
	return a.EventTime.Compare(b.EventTime)
}
