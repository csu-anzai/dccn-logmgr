package handler

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	pb "github.com/Ankr-network/dccn-es-api/proto/esmgr"
	"github.com/golang/glog"
	"github.com/olivere/elastic"
)

/*
Example:
GET _search
{
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "kubernetes.pod_name": "facade"
                    }
                }
            ],
            "filter": [
                {
                    "range": {
                        "@timestamp": {
                            "gte": "03/06/2019",
                            "lte": "now",
                            "format": "dd/MM/yyyy||yyyy",
                            "time_zone": "+08:00"
                        }
                    }
                }
            ]
        }
    }
}

or

GET _search
{
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "kubernetes.pod_name": "monitor"
                    }
                },
                {
                    "range": {
                        "@timestamp": {
                            "gte": "2019-05-30 00:00:00",
                            "lte": "2019-06-03 00:00:00",
                            "format": "yyyy-MM-dd HH:mm:ss",
                            "time_zone": "+08:00"
                        }
                    }
                }
            ]
        }
    },
    "size": 10,
    "search_after": [1559491200000],
    "sort": [
        {
          "@timestamp": "desc"
        }
    ]
}

Handle flow:
   1、 create es client
   2、 get request info and build query object
   3、 do request and transfer to response format
*/

const (
	ES_URL         = "http://elasticsearch:9200"
	PER_FETCH_SIZE = 1000
	CTX_REQID      = "ankr_req_id"
	TERM_APP       = "kubernetes.labels.app"
	TERM_POD       = "kubernetes.pod_name"
	TIME_ZONE      = "+08:00"
	TIME_FORMAT    = "yyyy-MM-dd HH:mm:ss"
	RANGE_FIELD    = "@timestamp"

	TEST_COUNT = 10
)

type Term struct {
	Name  string
	Value interface{}
}

var (
	ErrPingFailed = errors.New("failed to ping es node")
)

type EsMgrHandler struct {
	client *elastic.Client
	url    string
}

type HandlerOptionFunc func(*EsMgrHandler) error

func SetURL(url string) HandlerOptionFunc {
	return func(es *EsMgrHandler) error {
		es.url = url
		return nil
	}
}

func handleSearchResult(result *elastic.SearchResult, typ reflect.Type) []*pb.LogEntry {
	log_entrys := make([]*pb.LogEntry, 0, len(result.Hits.Hits))
	podID_Map := make(map[string]*pb.LogEntry)
	for _, item := range result.Each(typ) {
		if l, ok := item.(RawLogEntry); ok {
			log_entry := new(pb.LogEntry)
			var exist bool
			if log_entry, exist = podID_Map[l.Kubernetes.PodID]; !exist {
				log_entry = &pb.LogEntry{
					MetaData: &pb.LogMetaData{
						PodId:         l.Kubernetes.PodID,
						PodName:       l.Kubernetes.PodName,
						NamespaceId:   l.Kubernetes.NamespaceID,
						NamespaceName: l.Kubernetes.NamespaceName,
					},
					LogItems: make([]*pb.LogItem, 0),
				}
				podID_Map[l.Kubernetes.PodID] = log_entry
			} else {
				log_entry = podID_Map[l.Kubernetes.PodID]
			}
			log_entry.LogItems = append(log_entry.LogItems, &pb.LogItem{Timestamp: l.Timestamp, Msg: l.Log})
			log_entrys = append(log_entrys, log_entry)
		}
	}
	return log_entrys
}

func NewEsMgrHandler(options ...HandlerOptionFunc) (*EsMgrHandler, error) {
	es := &EsMgrHandler{url: ES_URL}
	for _, opt := range options {
		opt(es)
	}
	c, err := elastic.NewClient(elastic.SetURL(es.url))
	if err != nil {
		return nil, err
	}
	es.client = c
	return es, nil
}

func (s *EsMgrHandler) ok() bool {
	ctx := context.Background()
	info, code, err := s.client.Ping(s.url).Do(ctx)
	if err != nil {
		glog.Errorf("failed to ping es node, %v", err)
		return false
	}
	//TODO: DEBUG
	glog.V(3).Infof("es returned with code %d and version %s", code, info.Version.Number)
	return true
}

func (s *EsMgrHandler) getTotalHitsCount(ctx context.Context, q elastic.Query) int64 {
	count, err := s.client.Count().Query(q).Pretty(true).Do(ctx)
	if err != nil {
		glog.Errorf("failed to get total hits count, %v", err)
	}
	return count
}

func (s *EsMgrHandler) ListLogByAppName(ctx context.Context, req *pb.LogAppRequest) (*pb.LogAppResponse, error) {
	req_id := ctx.Value(CTX_REQID).(string)
	if !s.ok() {
		return &pb.LogAppResponse{ReqId: req_id, Code: int32(InternalErrCode), Msg: InternalErrCode.String()}, ErrPingFailed
	}

	start_time := time.Unix(int64(req.StartTime), 0).Format("2006-01-02 03:04:05")
	end_time := time.Unix(int64(req.EndTime), 0).Format("2006-01-02 03:04:05")

	q := elastic.NewBoolQuery().Must(elastic.NewTermQuery(TERM_APP, req.AppName),
		elastic.NewRangeQuery(RANGE_FIELD).Gte(start_time).Lte(end_time).Format(TIME_FORMAT).TimeZone(TIME_ZONE))

	//1. fetch total count
	//2. search without search_after field and stored last sort number
	//3. search with search_after field setting value as last sort number
	count := s.getTotalHitsCount(ctx, q)
	resp := &pb.LogAppResponse{
		ReqId:      req_id,
		Code:       0,
		LogDetails: make([]*pb.LogEntry, 0, count),
	}
	var (
		cnt_handled int64
		last_sort   interface{}
		ttyp        RawLogEntry
	)
	last_sort = req.EndTime * 1000 //ms
	if count > 0 {
		podID_Map := make(map[string]*pb.LogEntry)
		for cnt_handled < count && cnt_handled < TEST_COUNT {
			searchResult, err := s.client.Search().
				Size(PER_FETCH_SIZE).
				Sort(RANGE_FIELD, false).
				Query(q).
				SearchAfter(last_sort).
				Pretty(true).
				Do(ctx)
			if err != nil {
				source, _ := q.Source()
				data, _ := json.Marshal(source)
				glog.Errorf("failed to search after \n, query => %s, %v", data, err)
				return &pb.LogAppResponse{ReqId: req_id, Code: int32(SearchAfterErrCode), Msg: SearchAfterErrCode.String()}, err
			}
			if len(searchResult.Hits.Hits[len(searchResult.Hits.Hits)-1].Sort) == 1 {
				last_sort = searchResult.Hits.Hits[len(searchResult.Hits.Hits)-1].Sort[0]
			}
			logEntrys := handleSearchResult(searchResult, reflect.TypeOf(ttyp))
			//merge log entry
			for i, entry := range logEntrys {
				if _, exists := podID_Map[entry.MetaData.PodId]; !exists {
					podID_Map[entry.MetaData.PodId] = logEntrys[i]
				} else {
					if podID_Map[entry.MetaData.PodId].LogItems == nil {
						podID_Map[entry.MetaData.PodId].LogItems = make([]*pb.LogItem, 0, len(entry.LogItems))
					}
					podID_Map[entry.MetaData.PodId].LogItems = append(podID_Map[entry.MetaData.PodId].LogItems, entry.LogItems...)
				}
			}
			cnt_handled = cnt_handled + int64(len(searchResult.Hits.Hits))
		}
		for k, _ := range podID_Map {
			resp.LogDetails = append(resp.LogDetails, podID_Map[k])
		}
	}
	return resp, nil
}

/*
type LogPodRequest struct {
	ReqId                string   `protobuf:"bytes,1,opt,name=req_id,json=reqId,proto3" json:"req_id,omitempty"`
	PodName              string   `protobuf:"bytes,2,opt,name=pod_name,json=podName,proto3" json:"pod_name,omitempty"`
	Keywords             []string `protobuf:"bytes,3,rep,name=keywords,proto3" json:"keywords,omitempty"`
	StartTime            uint64   `protobuf:"varint,4,opt,name=start_time,json=startTime,proto3" json:"start_time,omitempty"`
	EndTime              uint64   `protobuf:"varint,5,opt,name=end_time,json=endTime,proto3" json:"end_time,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}
*/
func (s *EsMgrHandler) ListLogByPodName(ctx context.Context, req *pb.LogPodRequest) (*pb.LogPodResponse, error) {
	req_id := ctx.Value(CTX_REQID).(string)
	if !s.ok() {
		return &pb.LogPodResponse{ReqId: req_id, Code: int32(InternalErrCode), Msg: InternalErrCode.String()}, ErrPingFailed
	}
	start_time := time.Unix(int64(req.StartTime), 0).Format("2006-01-02 03:04:05")
	end_time := time.Unix(int64(req.EndTime), 0).Format("2006-01-02 03:04:05")

	glog.V(3).Infof("start_time: %s, end_time: %s", start_time, end_time)

	q := elastic.NewBoolQuery().Must(elastic.NewTermQuery(TERM_POD, req.PodName),
		elastic.NewRangeQuery(RANGE_FIELD).Gte(start_time).Lte(end_time).Format(TIME_FORMAT).TimeZone(TIME_ZONE))

	//1. fetch total count
	//2. search without search_after field and stored last sort number
	//3. search with search_after field setting value as last sort number
	count := s.getTotalHitsCount(ctx, q)
	resp := &pb.LogPodResponse{
		ReqId: req_id,
		Code:  0,
	}
	var (
		cnt_handled int64
		last_sort   interface{}
		ttyp        RawLogEntry
	)
	last_sort = req.EndTime * 1000 //ms
	if count > 0 {
		for cnt_handled < count {
			searchResult, err := s.client.Search().
				Size(PER_FETCH_SIZE).
				Sort(RANGE_FIELD, false).
				Query(q).
				SearchAfter(last_sort).
				Pretty(true).
				Do(ctx)
			if err != nil {
				source, _ := q.Source()
				data, _ := json.Marshal(source)
				glog.Errorf("failed to search after %v\n, query => %s", err, data)
				return &pb.LogPodResponse{ReqId: req_id, Code: int32(SearchAfterErrCode), Msg: SearchAfterErrCode.String()}, err
			}
			if len(searchResult.Hits.Hits[len(searchResult.Hits.Hits)-1].Sort) == 1 {
				last_sort = searchResult.Hits.Hits[len(searchResult.Hits.Hits)-1].Sort[0]
			}
			logEntrys := handleSearchResult(searchResult, reflect.TypeOf(ttyp))
			if len(logEntrys) > 0 {
				resp.LogDetails = logEntrys[0]

			}
			cnt_handled = cnt_handled + int64(len(searchResult.Hits.Hits))
		}
	}
	return resp, nil

}
