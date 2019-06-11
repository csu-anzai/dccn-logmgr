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
	"google.golang.org/grpc/metadata"
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
	//TERM_APP       = "kubernetes.labels.ankr_app_id"
	TERM_APP    = "kubernetes.labels.app"
	TERM_POD    = "kubernetes.pod_name"
	TERM_LOG    = "log"
	TIME_ZONE   = "+08:00"
	TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"
	RANGE_FIELD = "@timestamp"
)

var (
	ErrPingFailed  = errors.New("failed to ping es node")
	ErrQueryNil    = errors.New("query is nil")
	ErrCountFailed = errors.New("failed to get count")
)

type EsMgrHandler struct {
	client *elastic.Client
	url    string
	query  elastic.Query
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

func (s *EsMgrHandler) Ping() bool {
	return s.ok()
}

func (s *EsMgrHandler) ok() bool {
	ctx := context.Background()
	info, code, err := s.client.Ping(s.url).Do(ctx)
	if err != nil {
		glog.Errorf("failed to ping es node, %v", err)
		return false
	}
	glog.V(3).Infof("es returned with code %d and version %s", code, info.Version.Number)
	return true
}

func (s *EsMgrHandler) buildQuery(ctx context.Context, req interface{}) elastic.Query {
	var (
		start_time, end_time string
		term_key             string
		term_value           interface{}
		keywords             []string
	)
	switch req.(type) {
	case *pb.LogAppRequest:
		start_time = time.Unix(int64(req.(*pb.LogAppRequest).StartTime), 0).Format("2006-01-02 03:04:05")
		end_time = time.Unix(int64(req.(*pb.LogAppRequest).EndTime), 0).Format("2006-01-02 03:04:05")
		term_key = TERM_APP
		term_value = req.(*pb.LogAppRequest).AppId
		if len(req.(*pb.LogAppRequest).Keywords) > 0 {
			keywords = req.(*pb.LogAppRequest).Keywords
		}
	case *pb.LogPodRequest:
		start_time = time.Unix(int64(req.(*pb.LogPodRequest).StartTime), 0).Format("2006-01-02 03:04:05")
		end_time = time.Unix(int64(req.(*pb.LogPodRequest).EndTime), 0).Format("2006-01-02 03:04:05")
		term_key = TERM_POD
		term_value = req.(*pb.LogPodRequest).PodName
		if len(req.(*pb.LogPodRequest).Keywords) > 0 {
			keywords = req.(*pb.LogPodRequest).Keywords
		}
	case *pb.LogAppCountRequest:
		start_time = time.Unix(int64(req.(*pb.LogAppCountRequest).StartTime), 0).Format("2006-01-02 03:04:05")
		end_time = time.Unix(int64(req.(*pb.LogAppCountRequest).EndTime), 0).Format("2006-01-02 03:04:05")
		term_key = TERM_APP
		term_value = req.(*pb.LogAppCountRequest).AppId
		if len(req.(*pb.LogAppCountRequest).Keywords) > 0 {
			keywords = req.(*pb.LogAppCountRequest).Keywords
		}
	case *pb.LogPodCountRequest:
		start_time = time.Unix(int64(req.(*pb.LogPodCountRequest).StartTime), 0).Format("2006-01-02 03:04:05")
		end_time = time.Unix(int64(req.(*pb.LogPodCountRequest).EndTime), 0).Format("2006-01-02 03:04:05")
		term_key = TERM_POD
		term_value = req.(*pb.LogPodCountRequest).PodName
		if len(req.(*pb.LogPodCountRequest).Keywords) > 0 {
			keywords = req.(*pb.LogPodCountRequest).Keywords
		}
	}
	q := elastic.NewBoolQuery().Must(elastic.NewTermQuery(term_key, term_value),
		elastic.NewRangeQuery(RANGE_FIELD).Gte(start_time).Lte(end_time).Format(TIME_FORMAT).TimeZone(TIME_ZONE))
	if len(keywords) > 0 {
		key_querys := make([]elastic.Query, 0, len(keywords))
		for _, k := range keywords {
			key_querys = append(key_querys, elastic.NewTermQuery(TERM_LOG, k))
		}
		q = q.Should(key_querys...)
	}
	s.query = q

	//TODO: DEBUG
	source, _ := q.Source()
	data, _ := json.Marshal(source)
	glog.Infof("bool query => %s", data)
	return q
}

func (s *EsMgrHandler) getTotalHitsCount(ctx context.Context) (int64, error) {
	if s.query == nil {
		glog.Errorf("query must not be nil")
		return 0, ErrQueryNil
	}
	count, err := s.client.Count().Query(s.query).Pretty(true).Do(ctx)
	if err != nil {
		glog.Errorf("failed to get total hits count, %v", err)
		return 0, ErrCountFailed
	}
	return count, nil
}

func (s *EsMgrHandler) GetLogCountByAppId(ctx context.Context, req *pb.LogAppCountRequest) (*pb.LogAppCountResponse, error) {
	req_id := "Unknown"
	md, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		req_id = md[CTX_REQID][0]
	}
	if !s.ok() {
		return &pb.LogAppCountResponse{ReqId: req_id, Code: int32(InternalErrCode), Msg: InternalErrCode.String()}, ErrPingFailed
	}
	s.buildQuery(ctx, req)
	count, err := s.getTotalHitsCount(ctx)
	if err != nil {
		glog.Errorf("failed to get total count, %v", err)
		return &pb.LogAppCountResponse{ReqId: req_id, Code: int32(CountErrCode), Msg: CountErrCode.String()}, err
	}
	return &pb.LogAppCountResponse{ReqId: req_id, Code: 0, Msg: "SUCCESS", Count: uint64(count)}, nil
}

func (s *EsMgrHandler) GetLogCountByPodName(ctx context.Context, req *pb.LogPodCountRequest) (*pb.LogPodCountResponse, error) {
	req_id := "Unknown"
	md, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		req_id = md[CTX_REQID][0]
	}
	if !s.ok() {
		return &pb.LogPodCountResponse{ReqId: req_id, Code: int32(InternalErrCode), Msg: InternalErrCode.String()}, ErrPingFailed
	}
	s.buildQuery(ctx, req)
	count, err := s.getTotalHitsCount(ctx)
	if err != nil {
		glog.Errorf("failed to get total count, %v", err)
		return &pb.LogPodCountResponse{ReqId: req_id, Code: int32(CountErrCode), Msg: CountErrCode.String()}, err
	}
	return &pb.LogPodCountResponse{ReqId: req_id, Code: 0, Msg: "SUCCESS", Count: uint64(count)}, nil
}

func (s *EsMgrHandler) ListLogByAppId(ctx context.Context, req *pb.LogAppRequest) (*pb.LogAppResponse, error) {
	req_id := "Unknown"
	md, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		req_id = md[CTX_REQID][0]
	}

	if req != nil && req.IsTest {
		return &pb.LogAppResponse{ReqId: req_id, Code: int32(0), Msg: "SUCCESS"}, nil
	}
	if !s.ok() {
		return &pb.LogAppResponse{ReqId: req_id, Code: int32(InternalErrCode), Msg: InternalErrCode.String()}, ErrPingFailed
	}

	s.buildQuery(ctx, req)

	//1. fetch total count
	//2. search with search_after field, if req has not search_after value, use start_time as search_after value when sort is asc, use end_time as search_after value when sort is desc
	count, err := s.getTotalHitsCount(ctx)
	if err != nil {
		glog.Errorf("failed to get total count, %v", err)
		return &pb.LogAppResponse{ReqId: req_id, Code: int32(CountErrCode), Msg: CountErrCode.String()}, err
	}
	resp := &pb.LogAppResponse{
		ReqId:      req_id,
		Code:       0,
		TotalCount: uint64(count),
		LogDetails: make([]*pb.LogEntry, 0, count),
	}
	var (
		cnt_handled int64
		size        int
		last_sort   interface{}
		ttyp        RawLogEntry
	)

	if req.Size > PER_FETCH_SIZE {
		size = PER_FETCH_SIZE
	} else {
		size = int(req.GetSize())
	}

	sort := false //desc
	if req.Sort == "asc" {
		sort = true
	}

	search_after := req.GetSearchAfter()
	if search_after == 0 {
		if sort {
			search_after = req.GetStartTime() * 1000 //ms
		} else {
			search_after = req.GetEndTime() * 1000
		}
	}
	glog.V(3).Infof("listlogbyappid: size => %d, sort => %t, search_after: %d", size, sort, search_after)

	if count > 0 {
		podID_Map := make(map[string]*pb.LogEntry)
		flag := true
		for cnt_handled < int64(size) && flag {
			searchResult, err := s.client.Search().
				Size(size).
				Sort(RANGE_FIELD, sort).
				Query(s.query).
				SearchAfter(search_after).
				Pretty(true).
				Do(ctx)
			if err != nil {
				source, _ := s.query.Source()
				data, _ := json.Marshal(source)
				glog.Errorf("failed to search after %v\n, query => %s", err, data)
				return &pb.LogAppResponse{ReqId: req_id, Code: int32(SearchAfterErrCode), Msg: SearchAfterErrCode.String()}, err
			}
			if len(searchResult.Hits.Hits[len(searchResult.Hits.Hits)-1].Sort) == 1 {
				last_sort = searchResult.Hits.Hits[len(searchResult.Hits.Hits)-1].Sort[0]
			}

			//last query item count may be less than size, if happened, set flag to be true for break next loop
			if len(searchResult.Hits.Hits) < size {
				flag = false
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
		end, ok := last_sort.(float64)
		if ok {
			resp.LastSearchEnd = uint64(end)
		}
	}
	return resp, nil
}

func (s *EsMgrHandler) ListLogByPodName(ctx context.Context, req *pb.LogPodRequest) (*pb.LogPodResponse, error) {
	req_id := "Unknown"
	md, ok := metadata.FromOutgoingContext(ctx)
	glog.Infof("ok: %t, md: %v", ok, md)
	if ok {
		req_id = md[CTX_REQID][0]
	}
	//TEST
	if req != nil && req.IsTest {
		return &pb.LogPodResponse{ReqId: req_id, Code: int32(0), Msg: "SUCCESS"}, nil
	}

	if !s.ok() {
		return &pb.LogPodResponse{ReqId: req_id, Code: int32(InternalErrCode), Msg: InternalErrCode.String()}, ErrPingFailed
	}

	s.buildQuery(ctx, req)

	count, err := s.getTotalHitsCount(ctx)
	if err != nil {
		glog.Errorf("failed to get total count, %v", err)
		return &pb.LogPodResponse{ReqId: req_id, Code: int32(CountErrCode), Msg: CountErrCode.String()}, err
	}
	resp := &pb.LogPodResponse{
		ReqId:      req_id,
		Code:       0,
		TotalCount: uint64(count),
	}
	var (
		cnt_handled int64
		size        int
		last_sort   interface{}
		ttyp        RawLogEntry
	)

	if req.Size > PER_FETCH_SIZE {
		size = PER_FETCH_SIZE
	} else {
		size = int(req.GetSize())
	}

	sort := false //desc
	if req.Sort == "asc" {
		sort = true
	}

	search_after := req.GetSearchAfter()
	if search_after == 0 {
		if sort {
			search_after = req.GetStartTime() * 1000 //ms
		} else {
			search_after = req.GetEndTime() * 1000
		}
	}

	glog.V(3).Infof("listlogbypodname: size => %d, sort => %t, search_after: %d", size, sort, search_after)
	if count > 0 {
		flag := true
		for cnt_handled < int64(size) && flag {
			searchResult, err := s.client.Search().
				Size(size).
				Sort(RANGE_FIELD, sort).
				Query(s.query).
				SearchAfter(search_after).
				Pretty(true).
				Do(ctx)
			if err != nil {
				source, _ := s.query.Source()
				data, _ := json.Marshal(source)
				glog.Errorf("failed to search after %v\n, query => %s", err, data)
				return &pb.LogPodResponse{ReqId: req_id, Code: int32(SearchAfterErrCode), Msg: SearchAfterErrCode.String()}, err
			}

			if len(searchResult.Hits.Hits[len(searchResult.Hits.Hits)-1].Sort) == 1 {
				last_sort = searchResult.Hits.Hits[len(searchResult.Hits.Hits)-1].Sort[0]
				glog.Infof("last_sort: %v", searchResult.Hits.Hits[len(searchResult.Hits.Hits)-1].Sort)
			}
			if len(searchResult.Hits.Hits) < size {
				flag = false
			}
			logEntrys := handleSearchResult(searchResult, reflect.TypeOf(ttyp))
			if len(logEntrys) > 0 {
				resp.LogDetails = logEntrys[0]

			}
			cnt_handled = cnt_handled + int64(len(searchResult.Hits.Hits))
		}
		end, ok := last_sort.(float64)
		if ok {
			resp.LastSearchEnd = uint64(end)
		}
	}
	return resp, nil
}
