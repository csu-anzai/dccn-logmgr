package main

import (
	"flag"
	"google.golang.org/grpc/reflection"
	"log"
	"net"

	"github.com/Ankr-network/dccn-es-api/handler"
	pb "github.com/Ankr-network/dccn-es-api/proto/esmgr"
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
	lis, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	server, err := handler.NewEsMgrHandler()
	if err != nil {
		log.Fatalf("failed to create es client, %v", err)
	}
	pb.RegisterEsMgrServer(s, server)
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
