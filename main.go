package main

import (
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Ankr-network/dccn-common/pgrpc"
	"github.com/Ankr-network/dccn-common/pgrpc/util"
	pb "github.com/Ankr-network/dccn-common/protos/logmgr/v1/grpc"
	"github.com/Ankr-network/dccn-logmgr/collector"
	"github.com/Ankr-network/dccn-logmgr/handler"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	PORT        = ":50051"
	RELAY_PORT  = ":50052"
	METRIC_PORT = ":9090"
	DEBUG_PORT  = ":8080"
)

var (
	dcID, isDaemon, hubLogMgrAddr string
	httpClient                    *http.Client
	isDebug                       bool
)

const (
	TIMEOUT = 30 * time.Second
	VERSION = "v1.0.0"
	esURL   = "http://elasticsearch.logging:9200"
)

func init() {
	flag.Set("logtostderr", "true")
	flag.Set("v", "3")
	flag.Parse()
	dcID = os.Getenv("DAEMON_ID")
	isDaemon = os.Getenv("IS_DAEMON")
	httpClient = &http.Client{
		Timeout:   TIMEOUT,
		Transport: &http.Transport{},
	}
}

func debugHandler(w http.ResponseWriter, req *http.Request) {
	data := struct {
		Acknowledge bool `json:"acknowledge"`
		IsDebug     bool `json:"is_debug"`
	}{
		Acknowledge: false,
		IsDebug:     false,
	}

	res, _ := json.Marshal(data)
	if err := req.ParseForm(); err != nil {
		w.Write([]byte(res))
		return
	}

	switch req.Form.Get("v") {
	case "1":
		isDebug = true
	case "0":
		isDebug = false
	default:
		isDebug = true
	}

	data.Acknowledge = true
	data.IsDebug = isDebug
	res, _ = json.Marshal(data)
	w.Write([]byte(res))
}

func main() {
	log.Println(">>>>>>>>>>>>>    DCCN LogMgr Start    >>>>>>>>>>>>>>>>>")
	log.Println(">>>>>>>>>>>>>    Version: ", VERSION, "    >>>>>>>>>>>>>>>>>")
	server, err := handler.NewLogMgrHandler(esURL, dcID)
	if err != nil {
		log.Fatalf("failed to create es client, %v", err)
	}

	//prometheus collector
	go func(h *handler.LogMgrHandler) {
		log.Println(">>>>>>>>>>>>>    Start prometheus collector monitor    >>>>>>>>>>>")
		router := mux.NewRouter()
		prometheus.MustRegister(server)
		prometheus.MustRegister(collector.NewClusterHealth(httpClient, esURL))
		prometheus.MustRegister(collector.NewIndices(httpClient, esURL))
		prometheus.MustRegister(collector.NewNodes(httpClient, esURL))
		prometheus.MustRegister(collector.NewSnapshots(httpClient, esURL))
		router.Handle("/metric", promhttp.Handler())
		log.Fatal(http.ListenAndServe(METRIC_PORT, router))
	}(server)

	//debug
	go func() {
		log.Println(">>>>>>>>>>>>>      Debug      >>>>>>>>>>>>>")
		http.HandleFunc("/debug", debugHandler)
		log.Fatal(http.ListenAndServe(DEBUG_PORT, nil))
	}()
	server.Ping()

	// in daemon
	if isDaemon == "true" {
		log.Println("<<<<<<<<<<<    Daemon Side    >>>>>>>>>>>")
		hubLogMgrAddr = strings.TrimPrefix(hubLogMgrAddr, "http://")
		s := grpc.NewServer()
		pb.RegisterLogMgrServer(s, server)
		util.RegisterPingServer(s, new(util.Server))
		reflection.Register(s)

		lis, err := pgrpc.Listen("tcp", hubLogMgrAddr, dcID, func(conn *net.Conn, err error) {
			if err != nil {
				glog.Errorf("pgrpc onaccept faied, %v", err)
				return
			}
			log.Println("new relay connection from: ", (*conn).RemoteAddr().String())
		})
		if err != nil {
			log.Fatalf("failed to init pgrpc: %v", err)
		}
		log.Fatalf("pgrpc fail: %s", s.Serve(lis))
		return
	}

	// in hub
	log.Println("<<<<<<<<<<<<<    Hub Side    >>>>>>>>>>>>>>")
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
			if isDebug {
				glog.Infof("connectable clients: %s", strings.Join(keys, ","))
			}
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
