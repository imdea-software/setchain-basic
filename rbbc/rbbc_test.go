package rbbc

import (
	"fmt"
	"testing"

	"imdea.org/redbelly/bc"
	"imdea.org/redbelly/p2p"
	"imdea.org/redbelly/p2p/simulator"
	"imdea.org/redbelly/p2p/zeromq"
	"imdea.org/redbelly/types"
	"imdea.org/redbelly/vrb"

	vrbtypes "imdea.org/redbelly/vrb/types"
)

func TestNormalRunWithSimulator(t *testing.T) {
	tests := []uint{4, 10}
	for _, nnodes := range tests {
		t.Run(fmt.Sprintf("N=%d", nnodes), func(t *testing.T) {
			rbbcs := prepareSimNodes(nnodes)
			decidedValues := make(chan *types.Block, nnodes)
			for _, rbbc := range rbbcs {
				go func(rbbc *RBBCConsensus) {
					rbbc.Initiate()
					decidedValues <- <-rbbc.DecideChan
				}(rbbc)
			}
			counter := 0
			var block *types.Block
			for b := range decidedValues {
				counter++
				if counter == 1 {
					block = b
				} else {
					if !block.IsEqual(b) {
						t.Fatal("Decided for different blocks")
					}
					if counter == int(nnodes) {
						break
					}
				}
			}
		})
	}
}

func prepareSimNodes(n uint) []*RBBCConsensus {
	sim := simulator.NewSimulator(2000)
	routers := []*p2p.Router{}
	rbbcs := []*RBBCConsensus{}
	for i := 0; i < int(n); i++ {
		simNode, _ := sim.NewNode()
		routers = append(routers, p2p.NewRouter(simNode))
		routers[i].Start()
		rbbcs = append(rbbcs, newNode(routers[i], routers[i], types.ProposerIndex(i), n))
	}
	sim.Start()
	return rbbcs
}

func newNode(vrbNS vrbtypes.VRBNetworkService, bcNS bc.BCNetworkService, i types.ProposerIndex, n uint) *RBBCConsensus {
	vrb := vrb.NewVerifiedReliableBroadcast(vrbNS, types.ProposerIndex(i), n, (n-1)/3)
	vrb.Start()

	bc := bc.NewBinaryConsensus(bcNS, types.ProposerIndex(i), n, (n-1)/3)
	bc.Start()

	proposerIdentity, _ := types.NewProposerIdentity(types.ProposerIndex(i))
	mempool := types.NewLocalMempool()
	mempool.Push([]byte(fmt.Sprintf("TestRecord%d", i)))
	signer := NewProposalSigner(proposerIdentity, mempool)
	rbbc := NewRBBCConsensus(vrb, bc, n, (n-1)/3, signer)
	rbbc.Start()
	return rbbc
}

func TestNormalRunWithZeroMQUnicast(t *testing.T) {
	tests := []uint{7} // with 15 nodes some nodes cannot send msg to some other nodes. with 16 or more cannot create all the sockets
	for _, nnodes := range tests {
		t.Run(fmt.Sprintf("N=%d", nnodes), func(t *testing.T) {
			rbbcs := prepareZMQUNodes(nnodes)
			decidedValues := make(chan *types.Block, nnodes)
			for _, rbbc := range rbbcs {
				go func(rbbc *RBBCConsensus) {
					rbbc.Initiate()
					decidedValues <- <-rbbc.DecideChan
				}(rbbc)
			}
			counter := 0
			var block *types.Block
			for b := range decidedValues {
				counter++
				if counter == 1 {
					block = b
				} else {
					if !block.IsEqual(b) {
						t.Fatal("Decided for different blocks")
					}
					if counter == int(nnodes) {
						break
					}
				}
			}
		})
	}
}

func prepareZMQUNodes(n uint) []*RBBCConsensus {
	nss := []*zeromq.ZMQUNetworkService{}
	rbbcs := []*RBBCConsensus{}
	outAddrs := make([][]string, n, n)
	for i := 0; i < int(n); i++ {
		for j := 0; j < int(n); j++ {
			outAddrs[i] = append(outAddrs[i], fmt.Sprintf("tcp://localhost:%d", (4000+j)))
		}
		nss = append(nss, zeromq.NewZMQUNetworkService(i, int(n), outAddrs[i][:], fmt.Sprintf("tcp://*:%d", (4000+i)), types.Correct))
		nss[i].Start()
	}

	for _, ns := range nss { //TODO: put a timeout
		<-ns.PingAll()
	}

	for i, ns := range nss {
		router := p2p.NewRouter(ns)
		router.Start()
		rbbcs = append(rbbcs, newNode(router, router, types.ProposerIndex(i), n))
	}

	return rbbcs
}

//~ func TestNormalRunWithZeroMQ(t *testing.T) {
//~ tests := []uint{21} //with 22 does not work. I think some msg get lost.
//~ for _, nnodes := range tests {
//~ t.Run(fmt.Sprintf("N=%d", nnodes), func(t *testing.T) {
//~ rbbcs := prepareZMQNodes(nnodes)
//~ decidedValues := make(chan *types.Block, nnodes)
//~ for _, rbbc := range rbbcs {
//~ go func(rbbc *RBBCConsensus) {
//~ rbbc.Initiate()
//~ decidedValues <- <-rbbc.DecideChan
//~ }(rbbc)
//~ }
//~ counter := 0
//~ var block *types.Block
//~ for b := range decidedValues {
//~ counter++
//~ if counter == 1 {
//~ block = b
//~ } else {
//~ if !block.IsEqual(b) {
//~ t.Fatal("Decided for different blocks")
//~ }
//~ if counter == int(nnodes) {
//~ break
//~ }
//~ }
//~ }
//~ })
//~ }
//~ }

// func prepareZMQNodes(n uint) []*RBBCConsensus {
// 	endpoints := make([]string, n)

// 	for i := uint(0); i < n; i++ {
// 		endpoints[i] = fmt.Sprintf("%d", (7000 + i))
// 	}

// 	routers := []*p2p.Router{}
// 	rbbcs := []*RBBCConsensus{}
// 	for i := 0; i < int(n); i++ {
// 		zmq := zeromq.NewZMQNetworkService(fmt.Sprintf("tcp://*:%s", endpoints[i]), fmt.Sprintf("node%d", i))
// 		zmq.Start()
// 		for j := 0; j < int(n); j++ {
// 			err := zmq.Connect(fmt.Sprintf("tcp://localhost:%s", endpoints[j]))
// 			if err != nil {
// 				return nil
// 			}
// 		}
// 		routers = append(routers, p2p.NewRouter(zmq))
// 		routers[i].Start()
// 		rbbcs = append(rbbcs, newNode(routers[i], routers[i], types.ProposerIndex(i), n))
// 	}

// 	time.Sleep(5 * time.Second)

// 	return rbbcs
// }
