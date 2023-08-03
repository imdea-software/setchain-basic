package main

import (
	"flag"
	"fmt"
	"time"

	"imdea.org/redbelly/configuration"
	"imdea.org/redbelly/logging"
	"imdea.org/redbelly/p2p"
	"imdea.org/redbelly/p2p/zeromq"
	"imdea.org/redbelly/types"

	vrbtypes "imdea.org/redbelly/vrb/types"
)

var logger *logging.Logger

var nodeID int
var peersListPath string
var listeningPort int

func init() {
	flag.IntVar(&nodeID, "id", -1, "Index for the node")
	flag.StringVar(&peersListPath, "peers", "", "Path to the peers file")
	flag.IntVar(&listeningPort, "l", 4000, "Port for incoming peer connections")
	flag.Parse()

	logger = logging.NewLogger(fmt.Sprintf("Node%d", nodeID))
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

	ns := zeromq.NewZMQUNetworkService(
		nodeID,
		peerList.Count(),
		*peerList,
		fmt.Sprintf("tcp://*:%d", listeningPort),
		types.Correct,
	)
	ns.Start()

	<-ns.PingAll()

	proposer, _ := types.NewProposerIdentity(types.ProposerIndex(nodeID))
	packet := p2p.NewVRBPacket(
		vrbtypes.NewInitMessage(
			types.NewProposal(
				[]types.Transaction{},
				proposer,
				0),
			types.ProposerIndex(nodeID)))

	ns.Broadcast(packet)
	logger.Info.Printf("Sent test packet %v\n", packet)

	for i := 0; i < peerList.Count(); i++ {
		p := <-ns.Incoming()
		logger.Info.Printf("New packet arrived %d from %v\n", i, p.VRBPayload.ProposerIndex)
	}

	time.Sleep(5 * time.Second) //wait for other nodes to end.

	logger.Info.Println("Ended")
}
