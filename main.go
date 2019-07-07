package main

import (
	"flag"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/Ankr-network/dccn-common/pgrpc"
	"github.com/Ankr-network/dccn-common/pgrpc/util"
	pb "github.com/Ankr-network/dccn-common/protos/logmgr/v1/grpc"
	"github.com/Ankr-network/dccn-logmgr/handler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	PORT       = ":50051"
	RELAY_PORT = ":50052"
)

var dcID = os.Getenv("DAEMON_ID")
var isDaemon = os.Getenv("IS_DAEMON")
var hubLogMgrAddr string

func init() {
	flag.Set("logtostderr", "true")
	flag.Set("v", "3")
	flag.Parse()
}

func main() {
	log.Println("++++++++  dccn-es-api start  +++++++++++++")
	server, err := handler.NewLogMgrHandler(dcID)
	if err != nil {
		log.Fatalf("failed to create es client, %v", err)
	}
	server.Ping()

	// in daemon
	if isDaemon == "true" {
		log.Println("++++++   In daemon environment  ++++++++")
		hubLogMgrAddr = strings.TrimPrefix(hubLogMgrAddr, "http://")
		s := grpc.NewServer()
		pb.RegisterLogMgrServer(s, server)
		util.RegisterPingServer(s, new(util.Server))
		reflection.Register(s)

		lis, err := pgrpc.Listen("tcp", hubLogMgrAddr, dcID, func(conn *net.Conn, _ error) {
			log.Println("new relay connection from: ", (*conn).RemoteAddr().String())
		})
		if err != nil {
			log.Fatalf("failed to init pgrpc: %v", err)
		}
		log.Fatalf("pgrpc fail: %s", s.Serve(lis))
		return
	}

	// in hub
	log.Println("++++++   In hub environment  ++++++++")
	if err := pgrpc.InitClient("tcp", RELAY_PORT, nil, util.PingHook, grpc.WithInsecure()); err != nil {
		log.Fatalf("failed to init pgrpc: %v", err)
	}
	go func() {
		// force ping every 20s, for net flash case
		for range time.Tick(20 * time.Second) {
			var keys = []string{}
			pgrpc.Each(func(key string, ips []string, cc *grpc.ClientConn, err error) error {
				if err != nil {
					log.Printf("ping %s fail: %s", key, err)
				} else {
					keys = append(keys, key)
				}
				return err
			})
			log.Println("connectable clients:", keys)
		}
	}()

	s := grpc.NewServer()
	pb.RegisterLogMgrServer(s, server)
	reflection.Register(s)

	lis, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
