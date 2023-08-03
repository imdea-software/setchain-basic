package rb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"imdea.org/redbelly/logging"
	"imdea.org/redbelly/p2p"
	"imdea.org/redbelly/p2p/simulator"
)

func TestDelivery(t *testing.T) {
	tests := []uint{4}
	for _, nnodes := range tests {
		logging.DefaultLogLevel = logging.LogLevelVerbose
		t.Run(fmt.Sprintf("N=%d", nnodes), func(t *testing.T) {
			rbs := prepareSimNodes(nnodes)
			wg := &sync.WaitGroup{}
			for i, rb := range rbs {
				wg.Add(1)
				go func(rb *ReliableBroadcast, index int) {
					rb.Start()
					defer rb.Stop()
					err := rb.Broadcast([]byte(fmt.Sprintf("BroadcastNode%d", index)))
					if err != nil {
						t.Fail()
						wg.Done()
						return
					}
					received := make(map[string]uint)
					for p := range rb.DeliveryChan() {
						ph := p.Hash()
						received[string(ph[:])]++
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
				}(rb, i)
			}
			wg.Wait()
		})
	}
}

func TestAggregationDataStructure(t *testing.T) {
	tests := []uint{4}
	for _, nnodes := range tests {
		logging.DefaultLogLevel = logging.LogLevelVerbose
		t.Run(fmt.Sprintf("N=%d", nnodes), func(t *testing.T) {
			rbs := prepareSimNodes(nnodes)
			wg := &sync.WaitGroup{}
			for i, rb := range rbs {
				wg.Add(1)
				go func(rb *ReliableBroadcast, index int) {
					rb.Start()
					defer rb.Stop()
					aggrRB := NewAggregatedReliableBroadcast(1, 5*time.Second, rb)
					aggrRB.Start()
					defer aggrRB.Stop()
					aggrRB.Broadcast([]byte(fmt.Sprintf("BroadcastNode%d", index)))
					received := make(map[string]uint)
					for p := range aggrRB.DeliveryChan() {
						ph := p.Hash()
						received[string(ph[:])]++
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
				}(rb, i)
			}
			wg.Wait()
		})
	}
}

func prepareSimNodes(n uint) []*ReliableBroadcast {
	sim := simulator.NewSimulator(2000)
	routers := []*p2p.Router{}
	vrbs := []*ReliableBroadcast{}
	for i := 0; i < int(n); i++ {
		simNode, _ := sim.NewNode()
		routers = append(routers, p2p.NewRouter(simNode))
		routers[i].Start()
		vrbs = append(vrbs, NewReliableBroadcast(routers[i], n, (n-1)/3))
	}
	sim.Start()
	return vrbs
}
