package zeromq

import (
	"fmt"
	"log"
	"testing"

	"imdea.org/redbelly/p2p"
	"imdea.org/redbelly/types"

	vrbtypes "imdea.org/redbelly/vrb/types"
)

func TestBroadcastWithUnicast(t *testing.T) {
	const NNodes = 4
	services := [NNodes]*ZMQUNetworkService{}
	outAddr := [NNodes]string{}
	for i := range outAddr {
		outAddr[i] = fmt.Sprintf("tcp://127.0.0.1:%d", (5000 + i))
	}
	for i := range services {
		services[i] = NewZMQUNetworkService(i, NNodes, outAddr[:], fmt.Sprintf("tcp://*:%d", (5000+i)), types.Correct)
		services[i].Start()
	}
	for _, s := range services {
		<-s.PingAll() //TODO: put a timeout with select
	}
	log.Println("Services started and ready")
	packets := [NNodes]*p2p.Packet{}
	for i := range services {
		proposer, _ := types.NewProposerIdentity(types.ProposerIndex(i))
		packets[i] = p2p.NewVRBPacket(
			vrbtypes.NewInitMessage(
				types.NewProposal(
					[]types.Transaction{},
					proposer,
					0),
				types.ProposerIndex(i)))
		services[i].Broadcast(packets[i])
	}
	for _, s := range services {
		for range services {
			<-s.Incoming() //TODO: Check packets are equal to the sent ones
			log.Println("Message arrived")
		}
	}
}
