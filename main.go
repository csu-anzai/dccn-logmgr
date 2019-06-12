package main

import (
	"flag"
	"google.golang.org/grpc/reflection"
	"log"
	"net"

	pb "github.com/Ankr-network/dccn-common/protos/logmgr/v1/grpc"
	"github.com/Ankr-network/dccn-logmgr/handler"
	"google.golang.org/grpc"
)

const (
	PORT = ":50001"
)

func init() {
	flag.Set("logtostderr", "true")
	flag.Set("v", "3")
	flag.Parse()
}

func main() {
	log.Println("++++++++  dccn-es-api start  +++++++++++++")
	lis, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	server, err := handler.NewLogMgrHandler()
	if err != nil {
		log.Fatalf("failed to create es client, %v", err)
	}
	server.Ping()
	pb.RegisterLogMgrServer(s, server)
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
