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

var addr = "localhost:50001"

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
		ONE_DAY = int64(24 * time.Hour / time.Second)
	)
	start_time := uint64(time.Now().Unix() - ONE_DAY)
	end_time := uint64(time.Now().Unix())
	app_id := "dccn-erc20-monitor"
	//1 TEST
	if rsp, err := esClient.GetLogCountByAppId(reqIdContext, &pb.LogAppCountRequest{ReqId: "req_id", AppId: app_id, StartTime: start_time, EndTime: end_time, IsTest: true}); err != nil {
		log.Fatal(err.Error())
	} else {
		fmt.Printf("1: resp: %v\n", rsp)
	}

	//2 Search without keywords
	if rsp, err := esClient.GetLogCountByAppId(reqIdContext, &pb.LogAppCountRequest{ReqId: "req_id", AppId: app_id, StartTime: start_time, EndTime: end_time}); err != nil {
		log.Fatal(err.Error())
	} else {
		data, _ := json.MarshalIndent(rsp, "", "  ")
		fmt.Printf("2: resp json format: %s\n", data)
	}

	//3 Search with keywords
	if rsp, err := esClient.GetLogCountByAppId(reqIdContext, &pb.LogAppCountRequest{ReqId: "req_id", AppId: app_id, StartTime: start_time, EndTime: end_time, Keywords: []string{"error"}}); err != nil {
		log.Fatal(err.Error())
	} else {
		data, _ := json.MarshalIndent(rsp, "", "  ")
		fmt.Printf("3: resp json format: %s\n", data)
	}

}
