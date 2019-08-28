package main

import (
	"context"
	"log"

	pb "github.com/Ankr-network/dccn-common/protos/usermgr/v1/grpc"
	"google.golang.org/grpc"
)

var addr = "localhost:50051"

func main() {
	log.SetFlags(log.LstdFlags | log.Llongfile)
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	userClient := pb.NewUserMgrClient(conn)

	//md := metadata.New(map[string]string{})
	//ctx := metadata.NewOutgoingContext(context.Background(), md)
	//md, ok := metadata.FromOutgoingContext(ctx)
	//if ok {
	//} else {
	//}

	ctx := context.Background()
	email := "ximing@ankr.com"
	if rsp, err := userClient.FakeToken(ctx, &pb.FakeTokenRequest{Email: email}); err != nil {
		log.Fatal(err)
	} else {
		log.Printf("resp: %v\n", rsp)
	}
}
