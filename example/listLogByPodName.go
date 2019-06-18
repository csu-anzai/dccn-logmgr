package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	pb "github.com/Ankr-network/dccn-common/protos/logmgr/v1/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var addr = "localhost:50051"

const (
	CTX_REQID = "ankr_req_id"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Llongfile)
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err.Error())
	}
	defer func(conn *grpc.ClientConn) {
		if err := conn.Close(); err != nil {
			log.Println(err.Error())
		}
	}(conn)

	esClient := pb.NewLogMgrClient(conn)

	md := metadata.New(map[string]string{
		CTX_REQID: "req-001",
	})

	ctx := metadata.NewOutgoingContext(context.Background(), md)
	reqIdContext, cancel := context.WithTimeout(ctx, 20*time.Second)

	md, ok := metadata.FromOutgoingContext(reqIdContext)
	if ok {
		fmt.Printf("req_id: %s\n", md[CTX_REQID][0])
	} else {
		fmt.Println("req_id: unknown")
	}
	defer cancel()

	const (
		ONE_DAY  = int64(24 * time.Hour / time.Second)
		ONE_WEEK = int64(2 * 24 * time.Hour / time.Second)
	)
	start_time := uint64(time.Now().Unix() - ONE_WEEK)
	end_time := uint64(time.Now().Unix())
	pod_name := "dccn-erc20-monitor-6d6fdbf687-kzfdf"
	//1 TEST
	if rsp, err := esClient.ListLogByPodName(reqIdContext, &pb.LogPodRequest{ReqId: "req_id", PodName: pod_name, StartTime: start_time, EndTime: end_time, IsTest: true}); err != nil {
		log.Fatal(err.Error())
	} else {
		fmt.Printf("1: resp: %v\n", rsp)
	}

	var search_after uint64
	//2 First search
	req := &pb.LogPodRequest{ReqId: "req_id", PodName: pod_name, StartTime: start_time, EndTime: end_time, Size: 10, Keywords: []string{"error"}}
	data, _ := json.MarshalIndent(req, "", "  ")
	fmt.Printf("2: req json format => %s\n", data)
	if rsp, err := esClient.ListLogByPodName(reqIdContext, req); err != nil {
		log.Fatal(err.Error())
	} else {
		search_after = rsp.LastSearchEnd
		data, _ := json.MarshalIndent(rsp, "", "  ")
		fmt.Printf("2: resp json format: %s\n", data)
	}

	//3 Search witch search_after
	req = &pb.LogPodRequest{ReqId: "req_id", PodName: pod_name, StartTime: start_time, EndTime: end_time, Size: 10, SearchAfter: search_after, Keywords: []string{"error"}}
	data, _ = json.MarshalIndent(req, "", "  ")
	fmt.Printf("3: req json format => %s\n", data)
	if rsp, err := esClient.ListLogByPodName(reqIdContext, req); err != nil {
		log.Fatal(err.Error())
	} else {
		search_after = rsp.LastSearchEnd
		data, _ := json.MarshalIndent(rsp, "", "  ")
		fmt.Printf("3: resp json format: %s\n", data)
	}

	//4 Search with keywords
	req = &pb.LogPodRequest{ReqId: "req_id", PodName: pod_name, StartTime: start_time, EndTime: end_time, Size: 10, SearchAfter: search_after, Keywords: []string{"error"}}
	data, _ = json.MarshalIndent(req, "", "  ")
	fmt.Printf("4: req json format => %s\n", data)
	if rsp, err := esClient.ListLogByPodName(reqIdContext, req); err != nil {
		log.Fatal(err.Error())
	} else {
		data, _ := json.MarshalIndent(rsp, "", "  ")
		fmt.Printf("4: resp json format: %s\n", data)
	}

}
