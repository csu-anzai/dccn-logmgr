package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ErrFailedNodeStats = errors.New("failed to get node stats")
)

func getRoles(node NodeStatsNodeResponse) map[string]bool {
	// default settings (2.x) and map, which roles to consider
	roles := map[string]bool{
		"master": false,
		"data":   false,
		"ingest": false,
		"client": true,
	}
	// assumption: a 5.x node has at least one role, otherwise it's a 1.7 or 2.x node
	if len(node.Roles) > 0 {
		for _, role := range node.Roles {
			// set every absent role to false
			if _, ok := roles[role]; !ok {
				roles[role] = false
			} else {
				// if present in the roles field, set to true
				roles[role] = true
			}
		}
	} else {
		for role, setting := range node.Attributes {
			if _, ok := roles[role]; ok {
				if setting == "false" {
					roles[role] = false
				} else {
					roles[role] = true
				}
			}
		}
	}
	if len(node.HTTP) == 0 {
		roles["client"] = false
	}
	return roles
}

func createRoleMetric(role string) *nodeMetric {
	return &nodeMetric{
		Type: prometheus.GaugeValue,
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(NAMESPACE_ES, "nodes", "roles"),
			"Node roles",
			defaultRoleLabels, prometheus.Labels{"role": role},
		),
		Value: func(node NodeStatsNodeResponse) float64 {
			return 1.0
		},
		Labels: func(cluster string, node NodeStatsNodeResponse) []string {
			return []string{
				cluster,
				node.Host,
				node.Name,
			}
		},
	}
}

var (
	defaultNodeLabels               = []string{"cluster", "host", "name", "es_master_node", "es_data_node", "es_ingest_node", "es_client_node"}
	defaultRoleLabels               = []string{"cluster", "host", "name"}
	defaultThreadPoolLabels         = append(defaultNodeLabels, "type")
	defaultFilesystemDataLabels     = append(defaultNodeLabels, "mount", "path")
	defaultFilesystemIODeviceLabels = append(defaultNodeLabels, "device")
	defaultCacheLabels              = append(defaultNodeLabels, "cache")

	defaultNodeLabelValues = func(cluster string, node NodeStatsNodeResponse) []string {
		roles := getRoles(node)
		return []string{
			cluster,
			node.Host,
			node.Name,
			fmt.Sprintf("%t", roles["master"]),
			fmt.Sprintf("%t", roles["data"]),
			fmt.Sprintf("%t", roles["ingest"]),
			fmt.Sprintf("%t", roles["client"]),
		}
	}
	defaultThreadPoolLabelValues = func(cluster string, node NodeStatsNodeResponse, pool string) []string {
		return append(defaultNodeLabelValues(cluster, node), pool)
	}
	defaultFilesystemDataLabelValues = func(cluster string, node NodeStatsNodeResponse, mount string, path string) []string {
		return append(defaultNodeLabelValues(cluster, node), mount, path)
	}
	defaultFilesystemIODeviceLabelValues = func(cluster string, node NodeStatsNodeResponse, device string) []string {
		return append(defaultNodeLabelValues(cluster, node), device)
	}
	defaultCacheHitLabelValues = func(cluster string, node NodeStatsNodeResponse) []string {
		return append(defaultNodeLabelValues(cluster, node), "hit")
	}
	defaultCacheMissLabelValues = func(cluster string, node NodeStatsNodeResponse) []string {
		return append(defaultNodeLabelValues(cluster, node), "miss")
	}
)

type nodeMetric struct {
	Type   prometheus.ValueType
	Desc   *prometheus.Desc
	Value  func(node NodeStatsNodeResponse) float64
	Labels func(cluster string, node NodeStatsNodeResponse) []string
}

type gcCollectionMetric struct {
	Type   prometheus.ValueType
	Desc   *prometheus.Desc
	Value  func(gcStats NodeStatsJVMGCCollectorResponse) float64
	Labels func(cluster string, node NodeStatsNodeResponse, collector string) []string
}

type threadPoolMetric struct {
	Type   prometheus.ValueType
	Desc   *prometheus.Desc
	Value  func(threadPoolStats NodeStatsThreadPoolPoolResponse) float64
	Labels func(cluster string, node NodeStatsNodeResponse, breaker string) []string
}

type filesystemDataMetric struct {
	Type   prometheus.ValueType
	Desc   *prometheus.Desc
	Value  func(fsStats NodeStatsFSDataResponse) float64
	Labels func(cluster string, node NodeStatsNodeResponse, mount string, path string) []string
}

type filesystemIODeviceMetric struct {
	Type   prometheus.ValueType
	Desc   *prometheus.Desc
	Value  func(fsStats NodeStatsFSIOStatsDeviceResponse) float64
	Labels func(cluster string, node NodeStatsNodeResponse, device string) []string
}

type Nodes struct {
	client *http.Client
	url    string

	up prometheus.Gauge

	nodeMetrics           []*nodeMetric
	gcCollectionMetrics   []*gcCollectionMetric
	threadPoolMetrics     []*threadPoolMetric
	filesystemDataMetrics []*filesystemDataMetric
}

// NewNodes defines Nodes Prometheus metrics
func NewNodes(client *http.Client, url string) *Nodes {
	return &Nodes{
		client: client,
		url:    url,

		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(NAMESPACE_ES, "node_stats", "up"),
			Help: "Was the last scrape of the ElasticSearch nodes endpoint successful.",
		}),

		nodeMetrics: []*nodeMetric{
			//query cache
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "query_cache_memory_size_bytes"),
					"Query cache memory usage in bytes",
					defaultNodeLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Indices.QueryCache.MemorySize)
				},
				Labels: defaultNodeLabelValues,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "query_cache_total"),
					"Query cache total count",
					defaultNodeLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Indices.QueryCache.TotalCount)
				},
				Labels: defaultNodeLabelValues,
			},
			//query cache
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "query_cache_cache_size"),
					"Query cache cache size",
					defaultNodeLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Indices.QueryCache.CacheSize)
				},
				Labels: defaultNodeLabelValues,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "query_cache_cache_total"),
					"Query cache cache count",
					defaultNodeLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Indices.QueryCache.CacheCount)
				},
				Labels: defaultNodeLabelValues,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "query_cache_count"),
					"Query cache count",
					defaultCacheLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Indices.QueryCache.HitCount)
				},
				Labels: defaultCacheHitLabelValues,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "query_miss_count"),
					"Query miss count",
					defaultCacheLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Indices.QueryCache.MissCount)
				},
				Labels: defaultCacheMissLabelValues,
			},
			// request cache
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "request_cache_memory_size_bytes"),
					"Request cache memory usage in bytes",
					defaultNodeLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Indices.RequestCache.MemorySize)
				},
				Labels: defaultNodeLabelValues,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "request_cache_count"),
					"Request cache count",
					defaultCacheLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Indices.RequestCache.HitCount)
				},
				Labels: defaultCacheHitLabelValues,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "request_miss_count"),
					"Request miss count",
					defaultCacheLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Indices.RequestCache.MissCount)
				},
				Labels: defaultCacheMissLabelValues,
			},
			//search
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "search_query_time_seconds"),
					"Total search query time in seconds",
					defaultNodeLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Indices.Search.QueryTime) / 1000
				},
				Labels: defaultNodeLabelValues,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "search_query_total"),
					"Total number of queries",
					defaultNodeLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Indices.Search.QueryTotal)
				},
				Labels: defaultNodeLabelValues,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "search_fetch_time_seconds"),
					"Total search fetch time in seconds",
					defaultNodeLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Indices.Search.FetchTime) / 1000
				},
				Labels: defaultNodeLabelValues,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "search_fetch_total"),
					"Total number of fetches",
					defaultNodeLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Indices.Search.FetchTotal)
				},
				Labels: defaultNodeLabelValues,
			},

			//docs
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "docs"),
					"Count of documents on this node",
					defaultNodeLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Indices.Docs.Count)
				},
				Labels: defaultNodeLabelValues,
			},

			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "store_size_bytes"),
					"Current size of stored index data in bytes",
					defaultNodeLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Indices.Store.Size)
				},
				Labels: defaultNodeLabelValues,
			},

			//**********************
			//JVM heap and non-heap
			//**********************
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory", "used_bytes"),
					"JVM memory currently used by area",
					append(defaultNodeLabels, "area"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.HeapUsed)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "heap")
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory", "used_bytes"),
					"JVM memory currently used by area",
					append(defaultNodeLabels, "area"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.NonHeapUsed)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "non-heap")
				},
			},

			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory", "max_bytes"),
					"JVM memory max",
					append(defaultNodeLabels, "area"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.HeapMax)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "heap")
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory", "committed_bytes"),
					"JVM memory currently committed by area",
					append(defaultNodeLabels, "area"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.HeapCommitted)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "heap")
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory", "committed_bytes"),
					"JVM memory currently committed by area",
					append(defaultNodeLabels, "area"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.NonHeapCommitted)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "non-heap")
				},
			},
			//JVM memory pool
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory_pool", "used_bytes"),
					"JVM memory currently used by pool",
					append(defaultNodeLabels, "pool"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.Pools["young"].Used)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "young")
				},
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory_pool", "max_bytes"),
					"JVM memory max by pool",
					append(defaultNodeLabels, "pool"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.Pools["young"].Max)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "young")
				},
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory_pool", "peak_used_bytes"),
					"JVM memory peak used by pool",
					append(defaultNodeLabels, "pool"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.Pools["young"].PeakUsed)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "young")
				},
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory_pool", "peak_max_bytes"),
					"JVM memory peak max by pool",
					append(defaultNodeLabels, "pool"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.Pools["young"].PeakMax)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "young")
				},
			},
			//survivor
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory_pool", "used_bytes"),
					"JVM memory currently used by pool",
					append(defaultNodeLabels, "pool"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.Pools["survivor"].Used)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "survivor")
				},
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory_pool", "max_bytes"),
					"JVM memory max by pool",
					append(defaultNodeLabels, "pool"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.Pools["survivor"].Max)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "survivor")
				},
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory_pool", "peak_used_bytes"),
					"JVM memory peak used by pool",
					append(defaultNodeLabels, "pool"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.Pools["survivor"].PeakUsed)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "survivor")
				},
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory_pool", "peak_max_bytes"),
					"JVM memory peak max by pool",
					append(defaultNodeLabels, "pool"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.Pools["survivor"].PeakMax)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "survivor")
				},
			},
			//old
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory_pool", "used_bytes"),
					"JVM memory currently used by pool",
					append(defaultNodeLabels, "pool"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.Pools["old"].Used)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "old")
				},
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory_pool", "max_bytes"),
					"JVM memory max by pool",
					append(defaultNodeLabels, "pool"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.Pools["old"].Max)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "old")
				},
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory_pool", "peak_used_bytes"),
					"JVM memory peak used by pool",
					append(defaultNodeLabels, "pool"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.Pools["old"].PeakUsed)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "old")
				},
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_memory_pool", "peak_max_bytes"),
					"JVM memory peak max by pool",
					append(defaultNodeLabels, "pool"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.Mem.Pools["old"].PeakMax)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "old")
				},
			},

			//JVM buffer pool
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_buffer_pool", "used_bytes"),
					"JVM buffer currently used",
					append(defaultNodeLabels, "type"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.BufferPools["direct"].Used)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "direct")
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_buffer_pool", "used_bytes"),
					"JVM buffer currently used",
					append(defaultNodeLabels, "type"), nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.JVM.BufferPools["mapped"].Used)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse) []string {
					return append(defaultNodeLabelValues(cluster, node), "mapped")
				},
			},

			//file and file descriptors
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "process", "open_files_count"),
					"Open file descriptors",
					defaultNodeLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Process.OpenFD)
				},
				Labels: defaultNodeLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "process", "max_files_descriptors"),
					"Max file descriptors",
					defaultNodeLabels, nil,
				),
				Value: func(node NodeStatsNodeResponse) float64 {
					return float64(node.Process.MaxFD)
				},
				Labels: defaultNodeLabelValues,
			},
		},
		gcCollectionMetrics: []*gcCollectionMetric{
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_gc", "collection_seconds_count"),
					"Count of JVM GC runs",
					append(defaultNodeLabels, "gc"), nil,
				),
				Value: func(gcStats NodeStatsJVMGCCollectorResponse) float64 {
					return float64(gcStats.CollectionCount)
				},
				Labels: func(cluster string, node NodeStatsNodeResponse, collector string) []string {
					return append(defaultNodeLabelValues(cluster, node), collector)
				},
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "jvm_gc", "collection_seconds_sum"),
					"GC run time in seconds",
					append(defaultNodeLabels, "gc"), nil,
				),
				Value: func(gcStats NodeStatsJVMGCCollectorResponse) float64 {
					return float64(gcStats.CollectionTime) / 1000
				},
				Labels: func(cluster string, node NodeStatsNodeResponse, collector string) []string {
					return append(defaultNodeLabelValues(cluster, node), collector)
				},
			},
		},
		threadPoolMetrics: []*threadPoolMetric{
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "thread_pool", "completed_count"),
					"Thread Pool operations completed",
					defaultThreadPoolLabels, nil,
				),
				Value: func(threadPoolStats NodeStatsThreadPoolPoolResponse) float64 {
					return float64(threadPoolStats.Completed)
				},
				Labels: defaultThreadPoolLabelValues,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "thread_pool", "rejected_count"),
					"Thread Pool operations rejected",
					defaultThreadPoolLabels, nil,
				),
				Value: func(threadPoolStats NodeStatsThreadPoolPoolResponse) float64 {
					return float64(threadPoolStats.Rejected)
				},
				Labels: defaultThreadPoolLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "thread_pool", "active_count"),
					"Thread Pool threads active",
					defaultThreadPoolLabels, nil,
				),
				Value: func(threadPoolStats NodeStatsThreadPoolPoolResponse) float64 {
					return float64(threadPoolStats.Active)
				},
				Labels: defaultThreadPoolLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "thread_pool", "largest_count"),
					"Thread Pool largest threads count",
					defaultThreadPoolLabels, nil,
				),
				Value: func(threadPoolStats NodeStatsThreadPoolPoolResponse) float64 {
					return float64(threadPoolStats.Largest)
				},
				Labels: defaultThreadPoolLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "thread_pool", "queue_count"),
					"Thread Pool operations queued",
					defaultThreadPoolLabels, nil,
				),
				Value: func(threadPoolStats NodeStatsThreadPoolPoolResponse) float64 {
					return float64(threadPoolStats.Queue)
				},
				Labels: defaultThreadPoolLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "thread_pool", "threads_count"),
					"Thread Pool current threads count",
					defaultThreadPoolLabels, nil,
				),
				Value: func(threadPoolStats NodeStatsThreadPoolPoolResponse) float64 {
					return float64(threadPoolStats.Threads)
				},
				Labels: defaultThreadPoolLabelValues,
			},
		},
		filesystemDataMetrics: []*filesystemDataMetric{
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "filesystem_data", "available_bytes"),
					"Available space on block device in bytes",
					defaultFilesystemDataLabels, nil,
				),
				Value: func(fsStats NodeStatsFSDataResponse) float64 {
					return float64(fsStats.Available)
				},
				Labels: defaultFilesystemDataLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "filesystem_data", "free_bytes"),
					"Free space on block device in bytes",
					defaultFilesystemDataLabels, nil,
				),
				Value: func(fsStats NodeStatsFSDataResponse) float64 {
					return float64(fsStats.Free)
				},
				Labels: defaultFilesystemDataLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "filesystem_data", "size_bytes"),
					"Size of block device in bytes",
					defaultFilesystemDataLabels, nil,
				),
				Value: func(fsStats NodeStatsFSDataResponse) float64 {
					return float64(fsStats.Total)
				},
				Labels: defaultFilesystemDataLabelValues,
			},
		},
	}
}

func (c *Nodes) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.nodeMetrics {
		ch <- metric.Desc
	}

	for _, metric := range c.gcCollectionMetrics {
		ch <- metric.Desc
	}

	for _, metric := range c.threadPoolMetrics {
		ch <- metric.Desc
	}

	for _, metric := range c.filesystemDataMetrics {
		ch <- metric.Desc
	}

	ch <- c.up.Desc()
}

func (c *Nodes) fetchAndDecodeNodeStats() (nodeStatsResponse, error) {
	var nsr nodeStatsResponse

	res, err := c.client.Get(c.url + "/_nodes/stats")
	if err != nil {
		glog.Errorf("failed to get node stats, %v", err)
		return nsr, ErrFailedNodeStats
	}

	defer func() {
		err = res.Body.Close()
		if err != nil {
			glog.Errorf("failed to close http client")
		}
	}()

	if res.StatusCode != http.StatusOK {
		return nsr, ErrHttpStatus
	}

	if err := json.NewDecoder(res.Body).Decode(&nsr); err != nil {
		glog.Errorf("failed to decode node stats info, %v", err)
		return nsr, err
	}
	return nsr, nil
}

// Collect get nodes metric values
func (c *Nodes) Collect(ch chan<- prometheus.Metric) {
	nodeStatsResp, err := c.fetchAndDecodeNodeStats()
	if err != nil {
		c.up.Set(0)
		return
	}
	c.up.Set(1)

	for _, node := range nodeStatsResp.Nodes {
		roles := getRoles(node)

		for _, role := range []string{"master", "data", "client", "ingest"} {
			if roles[role] {
				metric := createRoleMetric(role)
				ch <- prometheus.MustNewConstMetric(
					metric.Desc,
					metric.Type,
					metric.Value(node),
					metric.Labels(nodeStatsResp.ClusterName, node)...,
				)
			}
		}

		for _, metric := range c.nodeMetrics {
			ch <- prometheus.MustNewConstMetric(
				metric.Desc,
				metric.Type,
				metric.Value(node),
				metric.Labels(nodeStatsResp.ClusterName, node)...,
			)
		}

		// GC Stats
		for collector, gcStats := range node.JVM.GC.Collectors {
			for _, metric := range c.gcCollectionMetrics {
				ch <- prometheus.MustNewConstMetric(
					metric.Desc,
					metric.Type,
					metric.Value(gcStats),
					metric.Labels(nodeStatsResp.ClusterName, node, collector)...,
				)
			}
		}

		// Thread Pool stats
		for pool, pstats := range node.ThreadPool {
			for _, metric := range c.threadPoolMetrics {
				ch <- prometheus.MustNewConstMetric(
					metric.Desc,
					metric.Type,
					metric.Value(pstats),
					metric.Labels(nodeStatsResp.ClusterName, node, pool)...,
				)
			}
		}

		// File System Data Stats
		for _, fsDataStats := range node.FS.Data {
			for _, metric := range c.filesystemDataMetrics {
				ch <- prometheus.MustNewConstMetric(
					metric.Desc,
					metric.Type,
					metric.Value(fsDataStats),
					metric.Labels(nodeStatsResp.ClusterName, node, fsDataStats.Mount, fsDataStats.Path)...,
				)
			}
		}
	}

	ch <- c.up
}
