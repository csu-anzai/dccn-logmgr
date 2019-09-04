package collector

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ErrFailedIndexStats = errors.New("failed to get index stats")
)

type labels struct {
	keys   func(...string) []string
	values func(*ClusterInfoResponse, ...string) []string
}

type indexMetric struct {
	Type   prometheus.ValueType
	Desc   *prometheus.Desc
	Value  func(indexStats IndexStatsIndexResponse) float64
	Labels labels
}

type shardMetric struct {
	Type   prometheus.ValueType
	Desc   *prometheus.Desc
	Value  func(data IndexStatsIndexShardsDetailResponse) float64
	Labels labels
}

type Indices struct {
	client *http.Client
	url    string

	up prometheus.Gauge

	indexMetrics []*indexMetric
	//shardMetrics []*shardMetric
}

// NewIndices defines Indices Prometheus metrics
func NewIndices(client *http.Client, url string) *Indices {
	indexLabels := labels{
		keys: func(...string) []string {
			return []string{"index", "cluster"}
		},
		values: func(clusterInfo *ClusterInfoResponse, s ...string) []string {
			if clusterInfo != nil {
				return append(s, clusterInfo.ClusterName)
			}
			return append(s, "unknown_cluster")
		},
	}

	//shardLabels := labels{
	//	keys: func(...string) []string {
	//		return []string{"index", "shard", "node", "cluster"}
	//	},
	//	values: func(clusterInfo *ClusterInfoResponse, s ...string) []string {
	//		if clusterInfo != nil {
	//			return append(s, clusterInfo.ClusterName)
	//		}
	//		return append(s, "unknown_cluster")
	//	},
	//}

	indices := &Indices{
		client: client,
		url:    url,

		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "up"),
			Help: "Was the last scrape of the ElasticSearch index endpoint successful.",
		}),

		indexMetrics: []*indexMetric{
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "docs_primary"),
					"Count of documents with only primary shards",
					indexLabels.keys(), nil,
				),
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "docs_total"),
					"Total count of documents",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.Docs.Count)
				},
				Labels: indexLabels,
			},

			//store
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "store_size_bytes_primary"),
					"Current total size of stored index data in bytes with only primary shards on all nodes",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Primaries.Store.SizeInBytes)
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "store_size_bytes_total"),
					"Current total size of stored index data in bytes with all shards on all nodes",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.Store.SizeInBytes)
				},
				Labels: indexLabels,
			},

			//segment
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "segment_count_primary"),
					"Current number of segments with only primary shards on all nodes",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Primaries.Segments.Count)
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "segment_count_total"),
					"Current number of segments with all shards on all nodes",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.Segments.Count)
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "segment_memory_bytes_primary"),
					"Current size of segments with only primary shards on all nodes in bytes",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Primaries.Segments.MemoryInBytes)
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "indices", "segment_memory_bytes_total"),
					"Current size of segments with all shards on all nodes in bytes",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.Segments.MemoryInBytes)
				},
				Labels: indexLabels,
			},

			//search
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "search_query_time_seconds_total"),
					"Total search query time in seconds",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.Search.QueryTimeInMillis) / 1000
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "search_query_total"),
					"Total number of queries",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.Search.QueryTotal)
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "search_fetch_time_seconds_total"),
					"Total search fetch time in seconds",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.Search.FetchTimeInMillis) / 1000
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "search_fetch_total"),
					"Total search fetch count",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.Search.FetchTotal)
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "search_scroll_time_seconds_total"),
					"Total search scroll time in seconds",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.Search.ScrollTimeInMillis) / 1000
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "search_scroll_current"),
					"Current search scroll count",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.Search.ScrollCurrent)
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "search_scroll_total"),
					"Total search scroll count",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.Search.ScrollTotal)
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "search_suggest_time_seconds_total"),
					"Total search suggest time in seconds",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.Search.SuggestTimeInMillis) / 1000
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "search_suggest_total"),
					"Total search suggest count",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.Search.SuggestTotal)
				},
				Labels: indexLabels,
			},
			//get time
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "get_time_seconds_total"),
					"Total get time in seconds",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.Get.TimeInMillis) / 1000
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "get_total"),
					"Total get count",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.Get.Total)
				},
				Labels: indexLabels,
			},

			//query cache
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "query_cache_memory_bytes_total"),
					"Total query cache memory bytes",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.QueryCache.MemorySizeInBytes)
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "query_cache_size"),
					"Total query cache size",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.QueryCache.CacheSize)
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "query_cache_hits_total"),
					"Total query cache hits count",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.QueryCache.HitCount)
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "query_cache_misses_total"),
					"Total query cache misses count",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.QueryCache.MissCount)
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "query_cache_caches_total"),
					"Total query cache caches count",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.QueryCache.CacheCount)
				},
				Labels: indexLabels,
			},

			//request cache
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "request_cache_memory_bytes_total"),
					"Total request cache memory bytes",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.RequestCache.MemorySizeInBytes)
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "request_cache_hits_total"),
					"Total request cache hits count",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.RequestCache.HitCount)
				},
				Labels: indexLabels,
			},
			{
				Type: prometheus.CounterValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "index_stats", "request_cache_misses_total"),
					"Total request cache misses count",
					indexLabels.keys(), nil,
				),
				Value: func(indexStats IndexStatsIndexResponse) float64 {
					return float64(indexStats.Total.RequestCache.MissCount)
				},
				Labels: indexLabels,
			},
		},
		//shardMetrics: []*shardMetric{
		//	{
		//		Type: prometheus.GaugeValue,
		//		Desc: prometheus.NewDesc(
		//			prometheus.BuildFQName(NAMESPACE_ES, "indices", "shared_docs"),
		//			"Count of documents on this shard",
		//			shardLabels.keys(), nil,
		//		),
		//		Value: func(data IndexStatsIndexShardsDetailResponse) float64 {
		//			return float64(data.Docs.Count)
		//		},
		//		Labels: shardLabels,
		//	},
		//},
	}

	return indices
}

func (i *Indices) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range i.indexMetrics {
		ch <- metric.Desc
	}
	ch <- i.up.Desc()
}

func (i *Indices) fetchAndDecodeIndexStats() (indexStatsResponse, error) {
	var isr indexStatsResponse

	res, err := i.client.Get(i.url + "/_all/_stats")
	if err != nil {
		glog.Errorf("failed to get index stats, %v", err)
		return isr, ErrFailedIndexStats
	}

	defer func() {
		err = res.Body.Close()
		if err != nil {
			glog.Errorf("failed to close http client, %v", err)
		}
	}()

	if res.StatusCode != http.StatusOK {
		return isr, ErrHttpStatus
	}

	if err := json.NewDecoder(res.Body).Decode(&isr); err != nil {
		glog.Errorf("failed to decode index stats, %v", err)
		return isr, err
	}
	return isr, nil
}

// Collect gets Indices metric values

func (i *Indices) Collect(ch chan<- prometheus.Metric) {
	indexStatsResp, err := i.fetchAndDecodeIndexStats()
	if err != nil {
		i.up.Set(0)
		glog.Errorf("failed to fetch and decode index stats, %v", err)
		return
	}

	i.up.Set(1)
	//index stats
	clusterInfo, err := GetClusterInfo(i.url)
	if err != nil {
		return
	}
	for indexName, indexStats := range indexStatsResp.Indices {
		glog.V(3).Infof("collect: index => %s, indexStats => %+v", indexName, indexStats)
		for _, metric := range i.indexMetrics {
			ch <- prometheus.MustNewConstMetric(
				metric.Desc,
				metric.Type,
				metric.Value(indexStats),
				metric.Labels.values(clusterInfo, indexName)...,
			)
		}
	}
	ch <- i.up
}
