package collector

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	NAMESPACE_ES = "elasticsearch"
)

var (
	colors                     = []string{"green", "yellow", "red"}
	defaultClusterHealthLabels = []string{"cluster"}

	ErrFailedClusterHealth = errors.New("failed to get cluster health")
	ErrHttpStatus          = errors.New("HTTP response code NOT OK")
	ErrFailedDataDecode    = errors.New("failed to decode cluster health data")
)

type clusterHealthMetric struct {
	Type  prometheus.ValueType
	Desc  *prometheus.Desc
	Value func(clusterHealth clusterHealthResponse) float64
}

type clusterHealthStatusMetric struct {
	Type   prometheus.ValueType
	Desc   *prometheus.Desc
	Value  func(clusterHealth clusterHealthResponse, color string) float64
	Labels func(clusterName, color string) []string
}

type ClusterHealth struct {
	client *http.Client
	url    string
	up     prometheus.Gauge

	metrics      []*clusterHealthMetric
	statusMetric *clusterHealthStatusMetric
}

//  NewClusterHealth returns a new Collector exposing ClusterHealth stats.
func NewClusterHealth(client *http.Client, url string) *ClusterHealth {
	subsystem := "cluster_health"
	return &ClusterHealth{
		client: client,
		url:    url,
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(NAMESPACE_ES, subsystem, "up"),
			Help: "Was the last scrape of the ElasticSearch cluster health endpoint successful.",
		}),

		metrics: []*clusterHealthMetric{
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, subsystem, "active_primary_shards"),
					"The number of primary shards in your cluster. This is an aggregate total across all indices.",
					defaultClusterHealthLabels, nil,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.ActivePrimaryShards)
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, subsystem, "active_shards"),
					"Aggregate total of all shards across all indices, which includes replica shards.",
					defaultClusterHealthLabels, nil,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.ActiveShards)
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, subsystem, "delayed_unassigned_shards"),
					"Shards delayed to reduce reallocation overhead",
					defaultClusterHealthLabels, nil,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.DelayedUnassignedShards)
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, subsystem, "number_of_data_nodes"),
					"Number of data nodes in the cluster.",
					defaultClusterHealthLabels, nil,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.NumberOfDataNodes)
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, subsystem, "unassigned_shards"),
					"The number of shards that exist in the cluster state, but cannot be found in the cluster itself.",
					defaultClusterHealthLabels, nil,
				),
				Value: func(clusterHealth clusterHealthResponse) float64 {
					return float64(clusterHealth.UnassignedShards)
				},
			},
		},
		statusMetric: &clusterHealthStatusMetric{
			Type: prometheus.GaugeValue,
			Desc: prometheus.NewDesc(
				prometheus.BuildFQName(NAMESPACE_ES, subsystem, "status"),
				"Whether all primary and replica shards are allocated.",
				[]string{"cluster", "color"}, nil,
			),
			Value: func(clusterHealth clusterHealthResponse, color string) float64 {
				if clusterHealth.Status == color {
					return 1
				}
				return 0
			},
		},
	}
}

func (c *ClusterHealth) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.metrics {
		ch <- metric.Desc
	}

	ch <- c.statusMetric.Desc
	ch <- c.up.Desc()
}

func (c *ClusterHealth) fetchAndDecodeClusterHealth() (clusterHealthResponse, error) {
	var chr clusterHealthResponse

	res, err := c.client.Get(c.url + "/_cluster/health")
	if err != nil {
		glog.Errorf("failed to get cluster health, %v", err)
		return chr, ErrFailedClusterHealth
	}
	defer func() {
		err = res.Body.Close()
		if err != nil {
			glog.Errorf("failed to close http client, %v", err)
		}
	}()

	if res.StatusCode != http.StatusOK {
		glog.Errorf("HTTP Request failed with code %d", res.StatusCode)
		return chr, ErrHttpStatus
	}

	if err := json.NewDecoder(res.Body).Decode(&chr); err != nil {
		glog.Errorf("failed to decode cluster health response, %v", err)
		return chr, ErrFailedDataDecode
	}
	return chr, nil
}

// Collect collects ClusterHealth metrics.
func (c *ClusterHealth) Collect(ch chan<- prometheus.Metric) {
	var err error

	glog.V(3).Infof("collect cluster health data, esURL => %s", c.url)
	clusterHealthResp, err := c.fetchAndDecodeClusterHealth()
	if err != nil {
		c.up.Set(0)
		glog.Errorf("failed to fetch and decode cluster health")
		return
	}
	c.up.Set(1)

	for _, metric := range c.metrics {
		ch <- prometheus.MustNewConstMetric(
			metric.Desc,
			metric.Type,
			metric.Value(clusterHealthResp),
			clusterHealthResp.ClusterName,
		)
	}

	for _, color := range colors {
		ch <- prometheus.MustNewConstMetric(
			c.statusMetric.Desc,
			c.statusMetric.Type,
			c.statusMetric.Value(clusterHealthResp, color),
			clusterHealthResp.ClusterName, color,
		)
	}
	ch <- c.up
}
