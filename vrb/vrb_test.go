package vrb

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"imdea.org/redbelly/p2p"
	"imdea.org/redbelly/p2p/simulator"
	"imdea.org/redbelly/types"
)

func TestDeliveryWithoutValidation(t *testing.T) {
	tests := []uint{4}
	for _, nnodes := range tests {
		t.Run(fmt.Sprintf("N=%d", nnodes), func(t *testing.T) {
			vrbs := prepareSimNodes(nnodes)
			wg := &sync.WaitGroup{}
			for i, vrb := range vrbs {
				wg.Add(1)
				go func(vrb *VerifiedReliableBroadcast, index int) {
					vrb.Start()
					defer vrb.Stop()
					identity, _ := types.NewProposerIdentity(types.ProposerIndex(index))
					tx := []byte(fmt.Sprintf("BroadcastNode%d", index))
					vrb.Broadcast(types.NewProposal([]types.Transaction{tx}, identity, 0))
					received := make(map[string]uint)
					for p := range vrb.DeliveryChan {
						received[string(p.Proposal.Txs[0])]++
						if len(received) == int(nnodes) {
							for _, v := range received {
								if v != 1 {
									t.Fail()
								}
							}
							break
						}
					}
					wg.Done()
				}(vrb, i)
			}
			wg.Wait()
		})
	}
}

func TestDeliveryWithStatelessValidation(t *testing.T) {
	tests := []uint{4}
	for _, nnodes := range tests {
		t.Run(fmt.Sprintf("N=%d", nnodes), func(t *testing.T) {
			vrbs := prepareSimNodes(nnodes)
			vu := &VerificationUtility{N: types.RBBCRound(nnodes), T: types.RBBCRound((nnodes - 1) / 3), V: isTrailingNumberOdd}
			wg := &sync.WaitGroup{}
			for i, vrb := range vrbs {
				wg.Add(1)
				vrbs[i].SetVerification(vu, 1*time.Second)
				go func(vrb *VerifiedReliableBroadcast, index int) {
					vrb.Start()
					defer vrb.Stop()
					identity, _ := types.NewProposerIdentity(types.ProposerIndex(index))
					txs := []types.Transaction{[]byte(fmt.Sprintf("BroadcastNode%d", index)), []byte(fmt.Sprintf("BroadcastNode%d", index+1))}
					vrb.Broadcast(types.NewProposal(txs, identity, 0))
					received := make(map[string]uint)
					for p := range vrb.DeliveryChan {
						types.PurgeTxList(p.Proposal.Txs, p.InvalidIndexes)
						received[string(p.Proposal.Txs[0])]++
						counter := uint(0)
						for _, k := range received {
							counter += k
						}
						if counter == nnodes {
							for tx := range received {
								if !strings.Contains(tx, "BroadcastNode") {
									fmt.Printf("%d|Something weird happened with transactions\n", index)
									t.Fail()
									continue
								}
								if isTrailingNumberOdd([]byte(tx)) {
									t.Fail()
								}
							}
							break
						}
					}
					wg.Done()
				}(vrb, i)
			}
			wg.Wait()
		})
	}
}

func prepareSimNodes(n uint) []*VerifiedReliableBroadcast {
	sim := simulator.NewSimulator(2000)
	routers := []*p2p.Router{}
	vrbs := []*VerifiedReliableBroadcast{}
	for i := 0; i < int(n); i++ {
		simNode, _ := sim.NewNode()
		routers = append(routers, p2p.NewRouter(simNode))
		routers[i].Start()
		vrbs = append(vrbs, NewVerifiedReliableBroadcast(routers[i], types.ProposerIndex(i), n, (n-1)/3))
	}
	sim.Start()
	return vrbs
}

func isTrailingNumberOdd(tx types.Transaction) bool {
	if k, err := strconv.Atoi(string(tx[len(tx)-1])); err == nil {
		return k%2 == 1
	} else {
		return false //should return error
	}
}
