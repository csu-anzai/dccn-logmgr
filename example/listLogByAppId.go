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
	CTX_DCID  = "ankr_dc_id"
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
		CTX_DCID:  "cls-e9242b31-3f8e-4d0a-b04f-913ff9f01ffe",
	})

	reqIdContext := metadata.NewOutgoingContext(context.Background(), md)

	md, ok := metadata.FromOutgoingContext(reqIdContext)
	if ok {
		fmt.Printf("req_id: %s\n", md[CTX_REQID][0])
	} else {
		fmt.Println("req_id: unknown")
	}
	//defer cancel()

	const (
		ONE_DAY = int64(24 * time.Hour / time.Second)
	)
	start_time := uint64(time.Now().Unix() - ONE_DAY)
	end_time := uint64(time.Now().Unix())
	app_id := "app-18607317-b7a9-4b0c-9e98-6ab2ae4178f7"
	//1 TEST
	if rsp, err := esClient.ListLogByAppId(reqIdContext, &pb.LogAppRequest{ReqId: "req_id", AppId: app_id, StartTime: start_time, EndTime: end_time, IsTest: true}); err != nil {
		log.Fatal(err.Error())
	} else {
		fmt.Printf("1: resp: %v\n", rsp)
	}

	var search_after uint64
	req := &pb.LogAppRequest{ReqId: "req_id", AppId: app_id, StartTime: start_time, EndTime: end_time, Size: 10}
	data, _ := json.MarshalIndent(req, "", "  ")
	fmt.Printf("2: req json format: %s\n", data)
	//2 First search
	if rsp, err := esClient.ListLogByAppId(reqIdContext, req); err != nil {
		log.Fatal(err.Error())
	} else {
		search_after = rsp.LastSearchEnd
		data, _ := json.MarshalIndent(rsp, "", "  ")
		fmt.Printf("2: resp json format: %s\n", data)
	}

	//3 Search witch search_after
	if rsp, err := esClient.ListLogByAppId(reqIdContext, &pb.LogAppRequest{ReqId: "req_id", AppId: app_id, StartTime: start_time, EndTime: end_time, Size: 10, SearchAfter: search_after}); err != nil {
		log.Fatal(err.Error())
	} else {
		search_after = rsp.LastSearchEnd
		data, _ := json.MarshalIndent(rsp, "", "  ")
		fmt.Printf("3: resp json format: %s\n", data)
	}

	//4 Search with keywords
	if rsp, err := esClient.ListLogByAppId(reqIdContext, &pb.LogAppRequest{ReqId: "req_id", AppId: app_id, StartTime: start_time, EndTime: end_time,
		Size: 100, SearchAfter: search_after, Keywords: []string{"error"}}); err != nil {
		log.Fatal(err.Error())
	} else {
		search_after = rsp.LastSearchEnd
		data, _ := json.MarshalIndent(rsp, "", "  ")
		fmt.Printf("4: resp json format: %s\n", data)
	}

}
