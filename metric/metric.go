package metric

import (
	"github.com/Ankr-network/dccn-logmgr/handler"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "logmgr"
)

type LogMgrCollector struct {
	esUp    *prometheus.Desc
	handler *handler.LogMgrHandler
}

func NewLogMgrCollector(h *handler.LogMgrHandler) *LogMgrCollector {

	return &LogMgrCollector{
		esUp: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "elasticsearch_up"),
			"Could Elasticsearch be reached in k8s cluster",
			nil,
			nil,
		),
		handler: h,
	}
}

func (c *LogMgrCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.esUp
}

func (c *LogMgrCollector) Collect(ch chan<- prometheus.Metric) {
	if c.handler == nil {
		return
	}
	if ok := c.handler.Ping(); ok {
		ch <- prometheus.MustNewConstMetric(c.esUp, prometheus.GaugeValue, 1)
	} else {
		ch <- prometheus.MustNewConstMetric(c.esUp, prometheus.GaugeValue, 0)
	}
	//metricValue := make(map[float64]uint64)
	//glog.V(3).Infof("metric:Collect, metricValue=%+v", metricValue)
	//glog.V(3).Infof("metric:Collect, ping result: %+v", c.handler.Ping())
	//ch <- prometheus.MustNewConstHistogram(c.logmgrMetric, uint64(1), 1, metricValue)
}
