package handler

/*
{
        "_index": "logstash-2019.05.29",
        "_type": "fluentd",
        "_id": "fn1SBGsBikKITH7YI4FG",
        "_score": null,
        "_source": {
          "log": "2019/05/29 16:00:13 heartbeat.go:48: 54.219.137.38: key 54.219.137.38 start \n",
          "stream": "stderr",
          "docker": {
            "container_id": "1563e51ffa62ad33132519cada15d77f6afb1993f0b578702720765473ad476a"
          },
          "kubernetes": {
            "container_name": "dccn-dc-facade",
            "namespace_name": "default",
            "pod_name": "dc-facade-547b7bcb56-2g6hx",
            "pod_id": "f5175624-8110-11e9-ab7f-06ad8f451fd2",
            "labels": {
              "app": "dc-facade",
              "pod-template-hash": "1036367612"
            },
            "host": "ip-172-20-40-235.us-west-1.compute.internal",
            "master_url": "https://100.64.0.1:443/api",
            "namespace_id": "c935fe26-2038-11e9-b097-06bbae7419fc",
            "namespace_labels": {
              "ankr_network": "true"
            }
          },
          "@timestamp": "2019-05-29T16:00:13.510930665+00:00",
          "tag": "kubernetes.var.log.containers.dc-facade-547b7bcb56-2g6hx_default_dccn-dc-facade-1563e51ffa62ad33132519cada15d77f6afb1993f0b578702720765473ad476a.log"
        },
        "sort": [
          1559145613510
        ]
      }
*/

type RawLogEntry struct {
	Log    string `json:"log"`
	Stream string `json:"stream"`
	Docker struct {
		ContainerID string `json:"container_id"`
	} `json:"docker"`
	Kubernetes struct {
		ContainerName   string `json:"container_name"`
		NamespaceName   string `json:"namespace_name"`
		NamespaceID     string `json:"namespace_id"`
		NamespaceLabels struct {
			AnkrNetwork string `json:"ankr_network"`
		}
		PodName string `json:"pod_name"`
		PodID   string `json:"pod_id"`
		Labels  struct {
			App             string `json:"app"`
			PodTemplateHash string `json:"pod-template-hash"`
		} `json:"labels"`
		Host      string `json:"host"`
		MasterUrl string `json:"master_url"`
	} `json:"kubernetes"`
	Timestamp string `json:"@timestamp"`
	Tag       string `json:"tag"`
}
