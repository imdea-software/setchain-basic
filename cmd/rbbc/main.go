package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"imdea.org/redbelly/configuration"
	"imdea.org/redbelly/logging"
	"imdea.org/redbelly/p2p"
	"imdea.org/redbelly/p2p/zeromq"
	"imdea.org/redbelly/rbbc"
	"imdea.org/redbelly/types"
)

var logger *logging.Logger

var nodeID int
var peersListPath string
var listeningPort int
var logConf logging.LogConf
var nTX int

func init() {
	flag.IntVar(&nodeID, "id", -1, "Index for the node")
	flag.StringVar(&peersListPath, "peers", "", "Path to the peers file")
	flag.IntVar(&listeningPort, "l", 4000, "Port for incoming peer connections")
	flag.IntVar(&nTX, "ntx", 50, "Number of 1KB test txs to send in a proposal")
	flag.Var(&logConf, "log", "Configure log for a component")
	flag.Parse()

	nodeIDString := fmt.Sprintf("Node%d", nodeID)

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
	ctx := context.Background()

	peerList, err := configuration.PeerListFromJsonFile(peersListPath)
	if err != nil {
		logger.Error.Fatalln("Error reading the peer list: ", err)
	}

	ns := zeromq.NewZMQUNetworkService(
		nodeID,
		peerList.Count(),
		*peerList,
		fmt.Sprintf("tcp://*:%d", listeningPort),
		types.Correct,
	)
	ns.Start()

	<-ns.PingAll()

	router := p2p.NewRouter(ns)
	router.Start()
	defer router.Stop()

	proposer, _ := types.NewProposerIdentity(types.ProposerIndex(nodeID))

	mempool := types.NewLocalMempool()
	mpCtx, mpCancel := context.WithCancel(ctx)
	defer mpCancel()
	go constantMempoolFiller(mpCtx, mempool, time.Millisecond, nTX)

	node := rbbc.NewNode(
		router,
		router,
		uint(peerList.Count()),
		(uint(peerList.Count())-1)/3,
		proposer,
		rbbc.NewProposalSigner(proposer, mempool), //temporary for testing
	)

	node.Start()
	defer node.Stop()

	logger.Info.Println("RBBC Started")

	managerCtx, managerCancel := context.WithCancel(ctx)
	defer managerCancel()
	go blockManager(managerCtx, node, mempool)

	node.RBBC.Initiate()

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt)
	<-stopChan
	logger.Info.Println("Stopping RBBC")
}

// test function to fulfill mempool
func randomMempoolFiller(ctx context.Context, m types.Mempool) {
loop:
	for {
		select {
		default:
			val, _ := rand.Int(rand.Reader, big.NewInt(1000000))
			m.Push([]byte(fmt.Sprintf("Record%s", val.String())))
			time.Sleep(time.Duration(val.Int64()) * time.Microsecond)
		case <-ctx.Done():
			break loop
		}
	}
}

// test function to fulfill mempool
func constantMempoolFiller(ctx context.Context, m types.Mempool, p time.Duration, q int) {
	ticker := time.NewTicker(p)
	maxRand := big.NewInt(1000000000)
	var val *big.Int
loop:
	for {
		select {
		case <-ticker.C:
			for i := 0; i < q; i++ {
				val, _ = rand.Int(rand.Reader, maxRand)
				m.Push([]byte(fmt.Sprintf("Record%s", val.String())))
			}
		case <-ctx.Done():
			break loop
		}
	}
}

// test function to fulfill mempool
func equalMempoolFiller(ctx context.Context, m types.Mempool, p time.Duration, q int) {
	ticker := time.NewTicker(p)
loop:
	for {
		select {
		case <-ticker.C:
			for i := 0; i < q; i++ {
				m.Push(make([]byte, 1024, 1024))
			}
		case <-ctx.Done():
			break loop
		}
	}
}

func blockManager(ctx context.Context, node *rbbc.Node, mempool types.Mempool) {
loop:
	for {
		select {
		case b := <-node.RBBC.DecideChan:
			mempool.Clean(b.Txs.Slice())
			logger.Info.Printf("Round: %d -> Block size: %d\n", b.R, b.Size())
			if node.RBBC.IsIdle() {
				node.RBBC.Initiate()
			}
		case <-ctx.Done():
			break loop
		}
	}
}
