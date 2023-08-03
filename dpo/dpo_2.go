package dpo

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"imdea.org/redbelly/dso"
	"imdea.org/redbelly/logging"
	"imdea.org/redbelly/p2p"
	"imdea.org/redbelly/rb"
	"imdea.org/redbelly/rbbc"
	"imdea.org/redbelly/types"
	"imdea.org/redbelly/vrb"
)

type DPO_2 struct {
	history    types.History
	nextEpoch  types.Epoch
	historyLog map[string]bool

	proposerIdentity *types.ProposerIdentity
	n                uint
	t                uint

	rb       *rb.ReliableBroadcast
	rbbcNode *rbbc.Node
	dso      dso.IDSO

	epochChangeLock *sync.RWMutex
	epochTimeout    time.Duration

	epochIncMsgQueue chan *Message

	stopChan chan interface{}

	logger *logging.Logger

	verificationFunc types.VerifyTransaction

	automaticEpochInc bool

	totalElementsStamped int64
	totalSetBroadcasted  int64
	totalAddedByClient   int64
	totalAddedByEpochInc int64
	totalTimetoStamp     time.Duration
}

func NewDPO_2(router *p2p.Router, n uint, t uint, pi *types.ProposerIdentity, vf types.VerifyTransaction, automaticEpochInc bool, dso dso.IDSO) *DPO_2 {
	dpo := &DPO_2{
		history:    types.History{},
		nextEpoch:  1,
		historyLog: make(map[string]bool),

		proposerIdentity: pi,
		n:                n,
		t:                t,

		rb:  rb.NewReliableBroadcast(router, n, t),
		dso: dso,

		epochChangeLock: &sync.RWMutex{},
		epochTimeout:    DefaultEpochTimeout,

		epochIncMsgQueue: make(chan *Message, ChannelBuffer),

		stopChan: make(chan interface{}),

		logger: logging.NewLogger("DPO_2"),

		verificationFunc: vf,

		automaticEpochInc: automaticEpochInc,

		totalElementsStamped: 0,
		totalSetBroadcasted:  0,
		totalAddedByClient:   0,
		totalAddedByEpochInc: 0,
		totalTimetoStamp:     time.Duration(0),
	}
	dpo.rbbcNode = rbbc.NewNode(router, router, n, t, pi, dpo)
	dpo.rbbcNode.RBBC.ReconciliateFunc = dpo.ReconciliateFunc
	vu := &vrb.VerificationUtility{N: types.RBBCRound(n), T: types.RBBCRound(t), V: vf}
	dpo.rbbcNode.VRB.SetVerification(vu, 100*time.Millisecond)

	return dpo
}

//TODO Use context to stop it
func (dpo *DPO_2) Start() {
	dpo.rb.Start()
	dpo.rbbcNode.Start()
	go dpo.incomingPayloadManager()
	go dpo.epochIncManager()
	go dpo.setDeliveryManager()

	if dpo.automaticEpochInc {
		go dpo.EpochInc(dpo.nextEpoch)
	}
}

func (dpo *DPO_2) Stop() {
	dpo.rb.Stop()
	dpo.rbbcNode.Stop()
	dpo.stopChan <- struct{}{}
	dpo.stopChan <- struct{}{}
	dpo.stopChan <- struct{}{}
}

func (dpo *DPO_2) Add(tx types.Transaction) (bool, error) {
	if !dpo.verificationFunc(tx) {
		return false, fmt.Errorf("invalid transaction %v", tx)
	}
	atomic.AddInt64(&dpo.totalAddedByClient, 1)
	return true, dpo.dso.Add(tx)
}

//TODO return a copy or TS version of history, not a pointer to internal data structures
func (dpo *DPO_2) Get() (*Storage, error) {
	s := &Storage{}
	dpo.epochChangeLock.RLock()
	s.History = dpo.history
	s.Epoch = dpo.nextEpoch - 1
	dpo.epochChangeLock.RUnlock()
	s.Set = types.TransactionSet{}
	var err error
	s.Set, err = dpo.dso.Get()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (dpo *DPO_2) GetTotalElementsAdded() int {
	set, _ := dpo.dso.Get() //TODO: Manage error
	return set.Size()
}

func (dpo *DPO_2) GetTotalElementsStamped() int64 {
	return dpo.totalElementsStamped
}

func (dpo *DPO_2) GetTotalAddedByEpochInc() int64 {
	return dpo.totalAddedByEpochInc
}

func (dpo *DPO_2) GetTotalSetBroadcasted() int64 {
	return dpo.totalSetBroadcasted
}

func (dpo *DPO_2) GetTotalAddedByClient() int64 {
	return dpo.totalAddedByClient
}

func (dpo *DPO_2) GetAvgTimeToStamp() float64 {
	return dpo.totalTimetoStamp.Seconds() / float64(dpo.totalElementsStamped)
}

func (dpo *DPO_2) EpochInc(e types.Epoch) error {
	dpo.epochChangeLock.RLock()
	defer dpo.epochChangeLock.RUnlock()
	if dpo.nextEpoch != e {
		return fmt.Errorf("can't increment to a non subsequent epoch (Current: %d, Requested: %d)", dpo.nextEpoch, e)
	}
	return dpo.rb.Broadcast(NewEpochIncPayload(e))
}

func (dpo *DPO_2) incomingPayloadManager() {
	for {
		select {
		case p := <-dpo.rb.DeliveryChan():
			msg := DecodePayload(p)
			switch msg.Type {
			case MTEpochInc:
				dpo.epochIncMsgQueue <- msg
			default:
				dpo.logger.Warning.Printf("Reliable broadcast delivered unrecognized message\n")
			}
		case <-dpo.stopChan:
			return
		}
	}
}

func (dpo *DPO_2) epochIncManager() {
	for {
		select {
		case msg := <-dpo.epochIncMsgQueue:
			if msg.Epoch < dpo.GetNextEpoch() {
				continue
			}
			if dpo.rbbcNode.RBBC.Initiate() { // TODO: should put a way to verify the EpochInc legitimacy
				dpo.logger.Info.Printf("Initiating increment to epoch %d\n", dpo.GetNextEpoch())
			}
		case <-dpo.stopChan:
			return
		}
	}
}

func (dpo *DPO_2) setDeliveryManager() {
	for {
		select {
		case block := <-dpo.rbbcNode.RBBC.DecideChan:
			dpo.logger.Debug.Printf("Processing increment to epoch %d\n", block.R)
			if block.R != types.RBBCRound(dpo.GetNextEpoch()) {
				dpo.logger.Error.Printf("Delivered non consecutive epoch. TODO: Manage this situation")
				continue
			}
			//TODO: Check element in epoch
			dpo.epochChangeLock.Lock()
			dpo.history[types.Epoch(block.R)] = types.TransactionSet{}
			accTimeToStamp := time.Duration(0)
			maxTimeToStamp := time.Duration(0)
			now := time.Now()
			for tx := range block.Txs { //MAYBE: clean from txs already in history
				t := []byte(tx)
				dpo.history[dpo.nextEpoch].Add(t)
				dpo.historyLog[tx] = true
				t_added, err := time.Parse(time.RFC3339Nano, string(t[96:]))
				if err != nil {
					dpo.logger.Error.Println(err, " while trying to parse", string(t[96:]))
				} else {
					t_timeToStamp := now.Sub(t_added)
					accTimeToStamp += t_timeToStamp
					maxTimeToStamp = maxDuration(maxTimeToStamp, t_timeToStamp)
				}
			}
			dpo.totalElementsStamped += int64(block.Txs.Size())
			dpo.totalTimetoStamp += accTimeToStamp
			dpo.logger.Debug.Printf("Epoch incremented to %d\n", dpo.nextEpoch)
			if BML != nil {
				BML.EpochIncremented(dpo.history[dpo.nextEpoch].Size(), accTimeToStamp, maxTimeToStamp)
			}
			dpo.nextEpoch++
			dpo.epochChangeLock.Unlock()
			if dpo.automaticEpochInc {
				go dpo.EpochInc(dpo.nextEpoch)
			}
		case <-dpo.stopChan:
			return
		}
	}
}

func (dpo *DPO_2) GetNextEpoch() types.Epoch {
	dpo.epochChangeLock.RLock()
	defer dpo.epochChangeLock.RUnlock()
	return dpo.nextEpoch
}

func (dpo *DPO_2) GetProposal(r types.RBBCRound) (*types.Proposal, *time.Time) {
	set, _ := dpo.dso.Get() //TODO: Manage error
	propSet := make([]types.Transaction, 0, set.Size())
	dpo.epochChangeLock.RLock()
	for t := range set {
		if !dpo.historyLog[t] {
			propSet = append(propSet, types.Transaction(t))
		}
	}
	dpo.epochChangeLock.RUnlock()
	p := types.NewProposal(
		propSet,
		dpo.proposerIdentity,
		types.RBBCRound(dpo.GetNextEpoch()),
	)
	timeout := time.Now().Add(dpo.epochTimeout)
	dpo.logger.Debug.Printf("Generated proposal %s for round %d\n", p.Hash.Fingerprint(), r)
	return p, &timeout
}

func (dpo *DPO_2) ReconciliateFunc(ps []*types.Proposal, currentRound types.RBBCRound) *types.Block {
	block := types.NewBlock(currentRound)
	txCounters := make(map[string]uint)
	for _, p := range ps {
		for _, tx := range p.Txs {
			txCounters[string(tx)]++
			if txCounters[string(tx)] > dpo.t {
				block.Txs.Add(tx)
			}
		}
	}
	return block
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
