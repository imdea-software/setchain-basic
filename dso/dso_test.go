package dso

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"imdea.org/redbelly/logging"
	"imdea.org/redbelly/p2p"
	"imdea.org/redbelly/p2p/simulator"
	"imdea.org/redbelly/rb"
	"imdea.org/redbelly/types"
)

func TestAdd(t *testing.T) {
	tests := []uint{4}
	for _, nnodes := range tests {
		logging.DefaultLogLevel = logging.LogLevelVerbose
		t.Run(fmt.Sprintf("N=%d", nnodes), func(t *testing.T) {
			rbs := prepareSimNodes(nnodes)
			wg := &sync.WaitGroup{}
			dsos := make([]*DSO, nnodes)
			for i, broadcast := range rbs {
				wg.Add(1)
				go func(reliableBroadcast *rb.ReliableBroadcast, index int) {
					reliableBroadcast.Start()
					dsos[index] = NewDSO(reliableBroadcast)
					dsos[index].Start()
					dsos[index].Add(types.Transaction(fmt.Sprintf("ADDNODE%d", index)))
					wg.Done()
				}(broadcast, i)
			}
			wg.Wait()
			time.Sleep(1 * time.Second) // waits eventual delivery
			for i, dso := range dsos {
				if dso.Get().Size() != int(nnodes) {
					t.Fail()
				}
				rbs[i].Stop()
				dso.Stop()
			}
		})
	}
}

func TestRPC(t *testing.T) {
	tests := []uint{4}
	for _, nnodes := range tests {
		logging.DefaultLogLevel = logging.LogLevelVerbose
		t.Run(fmt.Sprintf("N=%d", nnodes), func(t *testing.T) {
			rbs := prepareSimNodes(nnodes)
			wg := &sync.WaitGroup{}
			dsos := make([]*DSO, nnodes)
			for i, broadcast := range rbs {
				wg.Add(1)
				go func(reliableBroadcast *rb.ReliableBroadcast, index int) {
					reliableBroadcast.Start()
					dsos[index] = NewDSO(reliableBroadcast)
					dsos[index].Start()
					wg.Done()
				}(broadcast, i)
			}
			wg.Wait()
			server := NewDSOServer(dsos[0])
			server.Start(3000)
			time.Sleep(500 * time.Millisecond)
			client := NewDSOClient()
			err := client.Connect("localhost:3000")
			if err != nil {
				log.Println(err)
				t.FailNow()
			}
			err = client.Add(types.Transaction("ADDNODE"))
			if err != nil {
				log.Println(err)
				t.FailNow()
			}
			time.Sleep(2 * time.Second) // waits eventual delivery
			set, err := client.Get()
			if err != nil {
				log.Println(err)
				t.FailNow()
			}
			if set.Size() != 1 {
				log.Println(err)
				t.FailNow()
			}
		})
	}
}

func prepareSimNodes(n uint) []*rb.ReliableBroadcast {
	sim := simulator.NewSimulator(2000)
	routers := []*p2p.Router{}
	vrbs := []*rb.ReliableBroadcast{}
	for i := 0; i < int(n); i++ {
		simNode, _ := sim.NewNode()
		routers = append(routers, p2p.NewRouter(simNode))
		routers[i].Start()
		vrbs = append(vrbs, rb.NewReliableBroadcast(routers[i], n, (n-1)/3))
	}
	sim.Start()
	return vrbs
}
