package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"imdea.org/redbelly/configuration"
	dsolib "imdea.org/redbelly/dso"
	"imdea.org/redbelly/logging"
	"imdea.org/redbelly/p2p"
	"imdea.org/redbelly/p2p/zeromq"
	"imdea.org/redbelly/rb"
	"imdea.org/redbelly/types"
)

var logger *logging.Logger

var nodeID int
var peersListPath string
var listeningPort int
var rpcPort int
var logConf logging.LogConf
var silent int
var broadcastPeriod int

var nodeIDString string
var n uint
var f uint

var starting_time time.Time

func init() {
	flag.IntVar(&nodeID, "id", -1, "Index for the node")
	flag.StringVar(&peersListPath, "peers", "", "Path to the peers file")
	flag.IntVar(&listeningPort, "l", 4001, "Port for incoming peer connections")
	flag.IntVar(&rpcPort, "rpc", 3000, "Port for incoming rpc connections")
	flag.Var(&logConf, "log", "Configure log for a component")
	flag.IntVar(&silent, "silent", 0, "Number of silent servers. Must be less than n/3")
	flag.IntVar(&broadcastPeriod, "broadcastPeriod", 0, "Sets a periodic epoch increment request")
	flag.Parse()

	nodeIDString = fmt.Sprintf("DSONode%d", nodeID)

	for _, lc := range logConf {
		componentLog := strings.Split(lc, ":")
		logLevel, err := strconv.Atoi(componentLog[1])
		if err != nil || len(componentLog[0]) == 0 {
			log.Fatal("Can't recognize log level")
		}
		logging.LogLevels[componentLog[0]] = logLevel
	}

	logger = logging.NewLogger(nodeIDString)
	if nodeID == -1 {
		logger.Error.Fatalln("A node id must be provided. (-id)")
	}
	if peersListPath == "" {
		logger.Error.Fatalln("A peers list must be provided. (-peers)")
	}
}

func main() {
	var err error

	peerList, err := configuration.PeerListFromJsonFile(peersListPath)
	if err != nil {
		logger.Error.Fatalln("Error reading the peer list: ", err)
	}

	n = uint(peerList.Count())
	f = (n - 1) / 3

	if uint(silent) > f {
		logger.Error.Fatalln("Number of silent nodes too big. Must be less or equal than ", f)
	}
	behaviour := types.Correct
	if nodeID < silent {
		behaviour = types.Silent
	}

	ns := zeromq.NewZMQUNetworkService(
		nodeID,
		peerList.Count(),
		*peerList,
		fmt.Sprintf("tcp://*:%d", listeningPort),
		behaviour,
	)

	ns.Start()

	<-ns.PingAll()

	router := p2p.NewRouter(ns)
	router.Start()
	defer router.Stop()

	relBrd := rb.NewReliableBroadcast(router, n, f)
	relBrd.Start()
	defer relBrd.Stop()
	var dso *dsolib.DSO
	if broadcastPeriod != 0 {
		aggrRB := rb.NewAggregatedReliableBroadcast(1000000, time.Duration(broadcastPeriod)*time.Millisecond, relBrd)
		aggrRB.Start()
		defer aggrRB.Stop()
		dso = dsolib.NewDSO(aggrRB)
	} else {
		dso = dsolib.NewDSO(relBrd)
	}
	dso.Start()
	defer dso.Stop()
	server := dsolib.NewDSOServer(dso)
	server.Start(rpcPort)

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	<-stopChan

	logger.Info.Println("Stopping DSO")
}
