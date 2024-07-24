# gardener-scaling-common

> [!NOTE]
> Presently, this is 🚧 WIP prototype for Proof of Concept only.

A module that encapsulates 
1. Go Types that represent common scaling related entities of Gardener Kubernetes Cluster. See [API Types](./types.go)
2. Common Go-Client Utility functions in [Client Util](./clientutil/clientutil.go)

## Consumers

This is currently consumed by [Gardener Scaling History](https://github.com/elankath/gardener-scaling-history) and the [Gardener Virtual Autoscaler](https://github.com/elankath/gardener-scaling-history)

