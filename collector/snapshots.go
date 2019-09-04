package collector

import (
	"encoding/json"
	"net/http"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
)

type snapshotMetric struct {
	Type   prometheus.ValueType
	Desc   *prometheus.Desc
	Value  func(snapshotStats SnapshotStatDataResponse) float64
	Labels func(repositoryName string, snapshotStats SnapshotStatDataResponse) []string
}

type repositoryMetric struct {
	Type   prometheus.ValueType
	Desc   *prometheus.Desc
	Value  func(snapshotsStats SnapshotStatsResponse) float64
	Labels func(repositoryName string) []string
}

var (
	defaultSnapshotLabels      = []string{"repository", "state", "version"}
	defaultSnapshotLabelValues = func(repositoryName string, snapshotStats SnapshotStatDataResponse) []string {
		return []string{repositoryName, snapshotStats.State, snapshotStats.Version}
	}
	defaultSnapshotRepositoryLabels      = []string{"repository"}
	defaultSnapshotRepositoryLabelValues = func(repositoryName string) []string {
		return []string{repositoryName}
	}
)

type Snapshots struct {
	client *http.Client
	url    string

	up                prometheus.Gauge
	snapshotMetrics   []*snapshotMetric
	repositoryMetrics []*repositoryMetric
}

// NewSnapshots defines Snapshots Prometheus metrics
func NewSnapshots(client *http.Client, url string) *Snapshots {
	return &Snapshots{
		client: client,
		url:    url,

		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(NAMESPACE_ES, "snapshot_stats", "up"),
			Help: "Was the last scrape of the ElasticSearch snapshots endpoint successful.",
		}),

		snapshotMetrics: []*snapshotMetric{
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "snapshot_stats", "snapshot_number_of_indices"),
					"Number of indices in the last snapshot",
					defaultSnapshotLabels, nil,
				),
				Value: func(snapshotStats SnapshotStatDataResponse) float64 {
					return float64(len(snapshotStats.Indices))
				},
				Labels: defaultSnapshotLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "snapshot_stats", "snapshot_start_time_timestamp"),
					"Last snapshot start timestamp",
					defaultSnapshotLabels, nil,
				),
				Value: func(snapshotStats SnapshotStatDataResponse) float64 {
					return float64(snapshotStats.StartTimeInMillis / 1000)
				},
				Labels: defaultSnapshotLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "snapshot_stats", "snapshot_end_time_timestamp"),
					"Last snapshot end timestamp",
					defaultSnapshotLabels, nil,
				),
				Value: func(snapshotStats SnapshotStatDataResponse) float64 {
					return float64(snapshotStats.EndTimeInMillis / 1000)
				},
				Labels: defaultSnapshotLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "snapshot_stats", "snapshot_number_of_failures"),
					"Last snapshot number of failures",
					defaultSnapshotLabels, nil,
				),
				Value: func(snapshotStats SnapshotStatDataResponse) float64 {
					return float64(len(snapshotStats.Failures))
				},
				Labels: defaultSnapshotLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "snapshot_stats", "snapshot_total_shards"),
					"Last snapshot total shards",
					defaultSnapshotLabels, nil,
				),
				Value: func(snapshotStats SnapshotStatDataResponse) float64 {
					return float64(snapshotStats.Shards.Total)
				},
				Labels: defaultSnapshotLabelValues,
			},
			//need to monitor
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "snapshot_stats", "snapshot_failed_shards"),
					"Last snapshot failed shards",
					defaultSnapshotLabels, nil,
				),
				Value: func(snapshotStats SnapshotStatDataResponse) float64 {
					return float64(snapshotStats.Shards.Failed)
				},
				Labels: defaultSnapshotLabelValues,
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "snapshot_stats", "snapshot_successful_shards"),
					"Last snapshot successful shards",
					defaultSnapshotLabels, nil,
				),
				Value: func(snapshotStats SnapshotStatDataResponse) float64 {
					return float64(snapshotStats.Shards.Successful)
				},
				Labels: defaultSnapshotLabelValues,
			},
		},

		repositoryMetrics: []*repositoryMetric{
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(NAMESPACE_ES, "snapshot_stats", "number_of_snapshots"),
					"Number of snapshots in a repository",
					defaultSnapshotRepositoryLabels, nil,
				),
				Value: func(snapshotsStats SnapshotStatsResponse) float64 {
					return float64(len(snapshotsStats.Snapshots))
				},
				Labels: defaultSnapshotRepositoryLabelValues,
			},
		},
	}
}

// Describe add Snapshots metric descriptions
func (s *Snapshots) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range s.snapshotMetrics {
		ch <- metric.Desc
	}

	for _, metric := range s.repositoryMetrics {
		ch <- metric.Desc
	}

	ch <- s.up.Desc()
}

func (s *Snapshots) getRepositoryInfo(url string, data interface{}) error {
	res, err := s.client.Get(url)
	if err != nil {
		glog.Errorf("failed to get repository infomation, %v", err)
		return err
	}

	defer func() {
		err = res.Body.Close()
		if err != nil {
			glog.Errorf("failed to close http client")
		}
	}()

	if res.StatusCode != http.StatusOK {
		return ErrHttpStatus
	}

	if err := json.NewDecoder(res.Body).Decode(data); err != nil {
		glog.Errorf("failed to decode repository infomation, %v", err)
		return err
	}
	return nil
}

func (s *Snapshots) fetchAndDecodeSnapshotsStats() (map[string]SnapshotStatsResponse, error) {
	mssr := make(map[string]SnapshotStatsResponse)

	var srr SnapshotRepositoriesResponse

	err := s.getRepositoryInfo(s.url+"/_snapshot", &srr)
	if err != nil {
		return nil, err
	}

	for repo := range srr {
		var ssr SnapshotStatsResponse
		err := s.getRepositoryInfo(s.url+"/_snapshot"+repo+"/_all", &ssr)
		if err != nil {
			continue
		}
		mssr[repo] = ssr
	}

	return mssr, nil
}

// Collect gets Snapshots metric values
func (s *Snapshots) Collect(ch chan<- prometheus.Metric) {
	snapshotsStatsResp, err := s.fetchAndDecodeSnapshotsStats()
	if err != nil {
		s.up.Set(0)
		glog.Errorf("failed to get snapshots stats, %v", err)
		return
	}

	s.up.Set(1)

	for repositoryName, snapshotStats := range snapshotsStatsResp {
		for _, metric := range s.repositoryMetrics {
			ch <- prometheus.MustNewConstMetric(
				metric.Desc,
				metric.Type,
				metric.Value(snapshotStats),
				metric.Labels(repositoryName)...,
			)
		}
		if len(snapshotStats.Snapshots) == 0 {
			continue
		}

		lastSnapshot := snapshotStats.Snapshots[len(snapshotStats.Snapshots)-1]
		for _, metric := range s.snapshotMetrics {
			ch <- prometheus.MustNewConstMetric(
				metric.Desc,
				metric.Type,
				metric.Value(lastSnapshot),
				metric.Labels(repositoryName, lastSnapshot)...,
			)
		}
	}
	ch <- s.up
}
