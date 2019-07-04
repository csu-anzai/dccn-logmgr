package handler

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"github.com/Ankr-network/dccn-common/pgrpc"
	ankr_default "github.com/Ankr-network/dccn-common/protos"
	pb "github.com/Ankr-network/dccn-common/protos/logmgr/v1/grpc"
	"github.com/golang/glog"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
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
	ES_URL         = "http://elasticsearch.logging:9200"
	PER_FETCH_SIZE = 500
	CTX_REQID      = "ankr_req_id"
	TERM_APP       = "kubernetes.labels.release.keyword"
	//TERM_APP    = "kubernetes.labels.app.keyword"
	TERM_POD    = "kubernetes.pod_name.keyword"
	TERM_LOG    = "log"
	TIME_ZONE   = "+00:00"
	TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"
	RANGE_FIELD = "@timestamp"
	CTX_DC_ID   = "ankr_dc_id"
)

var (
	ErrPingFailed  = ankr_default.ErrElasticsearchPing
	ErrQueryNil    = ankr_default.ErrElasticsearchQuery
	ErrCountFailed = ankr_default.ErrElasticsearchCount
	ErrSearchAfter = ankr_default.ErrElasticsearchSearchAfter
)

type LogMgrHandler struct {
	client *elastic.Client
	url    string
	query  elastic.Query

	dcID string
}

type HandlerOptionFunc func(*LogMgrHandler) error

func SetURL(url string) HandlerOptionFunc {
	return func(es *LogMgrHandler) error {
		es.url = url
		return nil
	}
}

func handleSearchResult(result *elastic.SearchResult, typ reflect.Type) []*pb.LogItem {
	log_items := make([]*pb.LogItem, 0, len(result.Hits.Hits))
	for _, item := range result.Each(typ) {
		if l, ok := item.(RawLogEntry); ok {
			log_items = append(log_items, &pb.LogItem{
				MetaData: &pb.LogMetaData{
					PodId:         l.Kubernetes.PodID,
					PodName:       l.Kubernetes.PodName,
					NamespaceId:   l.Kubernetes.NamespaceID,
					NamespaceName: l.Kubernetes.NamespaceName,
				},
				Timestamp: l.Timestamp,
				Msg:       l.Log,
			})
		}
	}
	return log_items
}

func NewLogMgrHandler(dcID string, options ...HandlerOptionFunc) (*LogMgrHandler, error) {
	es := &LogMgrHandler{url: ES_URL, dcID: dcID}
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

func (s *LogMgrHandler) Ping() bool {
	return s.ok()
}

func (s *LogMgrHandler) ok() bool {
	ctx := context.Background()
	info, code, err := s.client.Ping(s.url).Do(ctx)
	if err != nil {
		glog.Errorf("failed to ping es node, %v", err)
		return false
	}
	glog.V(3).Infof("es returned with code %d and version %s", code, info.Version.Number)
	return true
}

func (s *LogMgrHandler) buildQuery(ctx context.Context, req interface{}) elastic.Query {
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
		kq := elastic.NewBoolQuery().Should(key_querys...)
		q = q.Must(kq)
	}
	s.query = q

	//TODO: DEBUG
	source, _ := q.Source()
	data, _ := json.Marshal(source)
	glog.Infof("bool query => %s", data)
	return q
}

func (s *LogMgrHandler) getTotalHitsCount(ctx context.Context) (int64, error) {
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

func (s *LogMgrHandler) GetLogCountByAppId(ctx context.Context, req *pb.LogAppCountRequest) (*pb.LogAppCountResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok && len(md[CTX_DC_ID]) != 0 && md[CTX_DC_ID][0] != s.dcID {
		conn, err := pgrpc.Dial(md[CTX_DC_ID][0])
		if err != nil {
			glog.Errorln(err)
			return nil, err
		}

		resp, err := pb.NewLogMgrClient(conn).GetLogCountByAppId(ctx, req)
		pgrpc.PutCC(conn, err)
		return resp, errors.Wrap(err, "remote fail")
	}

	req_id := "Unknown"
	if ok && len(md[CTX_REQID]) != 0 {
		req_id = md[CTX_REQID][0]
	}
	if req != nil && req.IsTest {
		return &pb.LogAppCountResponse{ReqId: req_id, Code: int32(SuccessCode), Msg: SuccessCode.String()}, nil
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
	return &pb.LogAppCountResponse{ReqId: req_id, Code: int32(SuccessCode), Msg: SuccessCode.String(), Count: uint64(count)}, nil
}

func (s *LogMgrHandler) GetLogCountByPodName(ctx context.Context, req *pb.LogPodCountRequest) (*pb.LogPodCountResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok && len(md[CTX_DC_ID]) != 0 && md[CTX_DC_ID][0] != s.dcID {
		conn, err := pgrpc.Dial(md[CTX_DC_ID][0])
		if err != nil {
			glog.Errorln(err)
			return nil, err
		}

		resp, err := pb.NewLogMgrClient(conn).GetLogCountByPodName(ctx, req)
		pgrpc.PutCC(conn, err)
		return resp, errors.Wrap(err, "remote fail")
	}

	req_id := "Unknown"
	if ok && len(md[CTX_REQID]) != 0 {
		req_id = md[CTX_REQID][0]
	}
	if req != nil && req.IsTest {
		return &pb.LogPodCountResponse{ReqId: req_id, Code: int32(SuccessCode), Msg: SuccessCode.String()}, nil
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
	return &pb.LogPodCountResponse{ReqId: req_id, Code: int32(SuccessCode), Msg: SuccessCode.String(), Count: uint64(count)}, nil
}

func (s *LogMgrHandler) ListLogByAppId(ctx context.Context, req *pb.LogAppRequest) (*pb.LogAppResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok && len(md[CTX_DC_ID]) != 0 && md[CTX_DC_ID][0] != s.dcID {
		conn, err := pgrpc.Dial(md[CTX_DC_ID][0])
		if err != nil {
			glog.Errorln(err)
			return nil, err
		}

		resp, err := pb.NewLogMgrClient(conn).ListLogByAppId(ctx, req)
		pgrpc.PutCC(conn, err)
		return resp, errors.Wrap(err, "remote fail")
	}

	req_id := "Unknown"
	if ok && len(md[CTX_REQID]) != 0 {
		req_id = md[CTX_REQID][0]
	}

	if req != nil && req.IsTest {
		return &pb.LogAppResponse{ReqId: req_id, Code: int32(SuccessCode), Msg: SuccessCode.String()}, nil
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
		Code:       int32(SuccessCode),
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
	glog.V(3).Infof("listlogbyappid: size => %d, sort => %t, search_after: %d", size, sort, search_after)

	if count > 0 {
		logItems := make([]*pb.LogItem, 0, size)
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

			logItems = append(logItems, handleSearchResult(searchResult, reflect.TypeOf(ttyp))...)
			cnt_handled = cnt_handled + int64(len(searchResult.Hits.Hits))
		}
		resp.LogItems = logItems
		end, ok := last_sort.(float64)
		if ok {
			resp.LastSearchEnd = uint64(end)
		}
	}
	return resp, nil
}

func (s *LogMgrHandler) ListLogByPodName(ctx context.Context, req *pb.LogPodRequest) (*pb.LogPodResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	glog.Infof("ok: %t, md: %v", ok, md)
	if ok && len(md[CTX_DC_ID]) != 0 && md[CTX_DC_ID][0] != s.dcID {
		conn, err := pgrpc.Dial(md[CTX_DC_ID][0])
		if err != nil {
			glog.Errorln(err)
			return nil, err
		}

		resp, err := pb.NewLogMgrClient(conn).ListLogByPodName(ctx, req)
		pgrpc.PutCC(conn, err)
		return resp, errors.Wrap(err, "remote fail")
	}

	req_id := "Unknown"
	if ok && len(md[CTX_REQID]) != 0 {
		req_id = md[CTX_REQID][0]
	}
	//TEST
	if req != nil && req.IsTest {
		return &pb.LogPodResponse{ReqId: req_id, Code: int32(SuccessCode), Msg: SuccessCode.String()}, nil
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
		Code:       int32(SuccessCode),
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
		logItems := make([]*pb.LogItem, 0, size)
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
				return &pb.LogPodResponse{ReqId: req_id, Code: int32(SearchAfterErrCode), Msg: SearchAfterErrCode.String()}, ErrSearchAfter
			}

			if len(searchResult.Hits.Hits[len(searchResult.Hits.Hits)-1].Sort) == 1 {
				last_sort = searchResult.Hits.Hits[len(searchResult.Hits.Hits)-1].Sort[0]
				glog.Infof("last_sort: %v", searchResult.Hits.Hits[len(searchResult.Hits.Hits)-1].Sort)
			}
			if len(searchResult.Hits.Hits) < size {
				flag = false
			}
			logItems = append(logItems, handleSearchResult(searchResult, reflect.TypeOf(ttyp))...)
			cnt_handled = cnt_handled + int64(len(searchResult.Hits.Hits))
		}
		resp.LogItems = append(resp.LogItems, logItems...)
		end, ok := last_sort.(float64)
		if ok {
			resp.LastSearchEnd = uint64(end)
		}
	}
	return resp, nil
}
