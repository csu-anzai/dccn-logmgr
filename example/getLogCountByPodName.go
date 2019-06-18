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

	reqIdContext := metadata.NewOutgoingContext(context.Background(), md)
	md, ok := metadata.FromOutgoingContext(reqIdContext)
	if ok {
		fmt.Printf("req_id: %s\n", md[CTX_REQID][0])
	} else {
		fmt.Println("req_id: unknown")
	}

	const (
		ONE_WEEK = int64(7 * 24 * time.Hour / time.Second)
	)
	start_time := uint64(time.Now().Unix() - ONE_WEEK)
	end_time := uint64(time.Now().Unix())
	pod_name := "monitor"
	//1 TEST
	if rsp, err := esClient.GetLogCountByPodName(reqIdContext, &pb.LogPodCountRequest{ReqId: "req_id", PodName: pod_name, StartTime: start_time, EndTime: end_time, IsTest: true}); err != nil {
		log.Fatal(err.Error())
	} else {
		fmt.Printf("1: resp: %v\n", rsp)
	}

	//2 Search without keywords
	if rsp, err := esClient.GetLogCountByPodName(reqIdContext, &pb.LogPodCountRequest{ReqId: "req_id", PodName: pod_name, StartTime: start_time, EndTime: end_time}); err != nil {
		log.Fatal(err.Error())
	} else {
		data, _ := json.MarshalIndent(rsp, "", "  ")
		fmt.Printf("2: resp json format: %s\n", data)
		fmt.Printf("2: resp raw format: %v\n", rsp)
	}

	//3 Search with keywords
	if rsp, err := esClient.GetLogCountByPodName(reqIdContext, &pb.LogPodCountRequest{ReqId: "req_id", PodName: pod_name, StartTime: start_time, EndTime: end_time, Keywords: []string{"error", "tendermint-2"}}); err != nil {
		log.Fatal(err.Error())
	} else {
		data, _ := json.MarshalIndent(rsp, "", "  ")
		fmt.Printf("3: resp json format: %s\n", data)
	}

}
