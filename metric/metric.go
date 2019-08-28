package metric

import (
	"github.com/Ankr-network/dccn-logmgr/handler"
	"github.com/prometheus/client_golang/prometheus"
)

type LogMgrCollector struct {
	logmgrMetric *prometheus.Desc
	handler      *handler.LogMgrHandler
}

func NewLogMgrCollector(h *handler.LogMgrHandler) *LogMgrCollector {

	return &LogMgrCollector{
		logmgrMetric: prometheus.NewDesc(
			"logmgr_metric",
			"Shows whether the logmgr work well in k8s cluster",
			nil,
			nil,
		),
		handler: h,
	}
}

func (c *LogMgrCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.logmgrMetric
}

func (c *LogMgrCollector) Collect(ch chan<- prometheus.Metric) {
	metricValue := make(map[float64]uint64)
	if c.handler == nil {
		return
	}
	if ok := c.handler.Ping(); ok {
		metricValue[float64(1)] = 1
	} else {
		metricValue[float64(1)] = 0
	}
	//glog.V(3).Infof("metric:Collect, metricValue=%+v", metricValue)
	//glog.V(3).Infof("metric:Collect, ping result: %+v", c.handler.Ping())
	ch <- prometheus.MustNewConstHistogram(c.logmgrMetric, uint64(1), 1, metricValue)
}
