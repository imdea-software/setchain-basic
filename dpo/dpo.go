package dpo

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"imdea.org/redbelly/logging"
	"imdea.org/redbelly/p2p"
	"imdea.org/redbelly/rb"
	"imdea.org/redbelly/rbbc"
	"imdea.org/redbelly/types"
	"imdea.org/redbelly/vrb"
)

const ChannelBuffer = 10000
const DefaultEpochTimeout = 10000 * time.Millisecond

var BML *logging.BenchmarkLogger

type Storage struct {
	History types.History
	Set     types.TransactionSet
	Epoch   types.Epoch
}

type DPO struct {
	Current        *sync.Map
	currentCounter int64
	history        types.History
	nextEpoch      types.Epoch

	proposerIdentity *types.ProposerIdentity

	rb       *rb.ReliableBroadcast
	rbbcNode *rbbc.Node

	epochChangeLock *sync.RWMutex
	epochTimeout    time.Duration

	addMsgQueue      chan *Message
	epochIncMsgQueue chan *Message
	setAddMsgQueue   chan *Message

	stopChan chan interface{}

	logger *logging.Logger

	verificationFunc types.VerifyTransaction

	pendingToBeBroadcasted        types.SetToBroadcast
	pendingToBeBroadcasted_locker sync.Mutex
	broadcastType                 types.BroadcastType
	broadcastPeriod               time.Duration

	automaticEpochInc bool

	totalElementsAdded   int64
	totalElementsStamped int64
	totalSetBroadcasted  int64
	totalAddedByClient   int64
	totalAddedByEpochInc int64
	totalTimetoStamp     time.Duration
}

func NewDPO(router *p2p.Router, n uint, t uint, pi *types.ProposerIdentity, vf types.VerifyTransaction, bt types.BroadcastType, bp time.Duration, automaticEpochInc bool, vt time.Duration) *DPO {
	dpo := &DPO{
		Current:        &sync.Map{},
		currentCounter: 0,
		history:        types.History{},
		nextEpoch:      1,

		proposerIdentity: pi,

		rb: rb.NewReliableBroadcast(router, n, t),

		epochChangeLock: &sync.RWMutex{},
		epochTimeout:    DefaultEpochTimeout,

		addMsgQueue:      make(chan *Message, ChannelBuffer),
		epochIncMsgQueue: make(chan *Message, ChannelBuffer),
		setAddMsgQueue:   make(chan *Message, ChannelBuffer),

		stopChan: make(chan interface{}),

		logger: logging.NewLogger("DPO"),

		verificationFunc: vf,

		pendingToBeBroadcasted_locker: sync.Mutex{},
		broadcastType:                 bt,
		broadcastPeriod:               bp,

		automaticEpochInc: automaticEpochInc,

		totalElementsAdded:   0,
		totalElementsStamped: 0,
		totalSetBroadcasted:  0,
		totalAddedByClient:   0,
		totalAddedByEpochInc: 0,
		totalTimetoStamp:     time.Duration(0),
	}
	dpo.rbbcNode = rbbc.NewNode(router, router, n, t, pi, dpo)
	dpo.rbbcNode.RBBC.ReconciliateFunc = dpo.ReconciliateFunc
	vu := &vrb.VerificationUtility{N: types.RBBCRound(n), T: types.RBBCRound(t), V: vf}
	dpo.rbbcNode.VRB.SetVerification(vu, vt*time.Millisecond)
	dpo.pendingToBeBroadcasted = types.NewSetToBroadcast(dpo.broadcastType)

	return dpo
}

//TODO Use context to stop it
func (dpo *DPO) Start() {
	dpo.rb.Start()
	dpo.rbbcNode.Start()
	go dpo.incomingPayloadManager()
	go dpo.epochIncManager()
	go dpo.addManager()
	go dpo.setDeliveryManager()
	go dpo.setAddManager()

	if dpo.broadcastType == types.BTPeriodic {
		go dpo.periodicBroadcastSet()
	}
	if dpo.automaticEpochInc {
		go dpo.EpochInc(dpo.nextEpoch)
	}
}

func (dpo *DPO) Stop() {
	dpo.rb.Stop()
	dpo.rbbcNode.Stop()
	dpo.stopChan <- struct{}{}
	dpo.stopChan <- struct{}{}
	dpo.stopChan <- struct{}{}
	dpo.stopChan <- struct{}{}
	dpo.stopChan <- struct{}{}
	if dpo.broadcastType == types.BTPeriodic {
		dpo.stopChan <- struct{}{}
	}
}

func (dpo *DPO) Add(tx types.Transaction) (bool, error) {
	if !dpo.verificationFunc(tx) {
		return false, fmt.Errorf("invalid transaction %v", tx)
	}

	return dpo.addToCurrent(tx, true)
}

//TODO return a copy or TS version of history, not a pointer to internal data structures
func (dpo *DPO) Get() (*Storage, error) {
	s := &Storage{}
	dpo.epochChangeLock.RLock()
	s.History = dpo.history
	s.Epoch = dpo.nextEpoch - 1
	dpo.epochChangeLock.RUnlock()
	s.Set = types.TransactionSet{}
	dpo.Current.Range(func(key, value interface{}) bool {
		s.Set.Add(types.Transaction(key.(string)))
		return true
	})
	return s, nil //TODO: manage errors
}

func (dpo *DPO) GetCurrentLength() int64 {
	return atomic.LoadInt64(&dpo.currentCounter)
}

func (dpo *DPO) GetTotalElementsAdded() int64 {
	return atomic.LoadInt64(&dpo.totalElementsAdded)
}

func (dpo *DPO) GetTotalElementsStamped() int64 {
	return dpo.totalElementsStamped
}

func (dpo *DPO) GetTotalAddedByEpochInc() int64 {
	return dpo.totalAddedByEpochInc
}

func (dpo *DPO) GetTotalSetBroadcasted() int64 {
	return dpo.totalSetBroadcasted
}

func (dpo *DPO) GetTotalAddedByClient() int64 {
	return dpo.totalAddedByClient
}

func (dpo *DPO) GetAvgTimeToStamp() float64 {
	return dpo.totalTimetoStamp.Seconds() / float64(dpo.totalElementsStamped)
}

func (dpo *DPO) EpochInc(e types.Epoch) error {
	dpo.epochChangeLock.RLock()
	defer dpo.epochChangeLock.RUnlock()
	if dpo.nextEpoch != e {
		return fmt.Errorf("can't increment to a non subsequent epoch (Current: %d, Requested: %d)", dpo.nextEpoch, e)
	}
	return dpo.rb.Broadcast(NewEpochIncPayload(e))
}

func (dpo *DPO) incomingPayloadManager() {
	for {
		select {
		case p := <-dpo.rb.DeliveryChan():
			msg := DecodePayload(p)
			switch msg.Type {
			case MTEpochInc:
				dpo.epochIncMsgQueue <- msg
			case MTAdd:
				dpo.addMsgQueue <- msg
			case MTSetAdd:
				dpo.setAddMsgQueue <- msg
			}
		case <-dpo.stopChan:
			return
		}
	}
}

func (dpo *DPO) epochIncManager() {
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

func (dpo *DPO) addManager() {
	for {
		select {
		case msg := <-dpo.addMsgQueue:
			if !dpo.verificationFunc(msg.Transaction) {
				dpo.logger.Verbose.Printf("Discarded invalid transaction %s\n", msg.Transaction.Hash().Fingerprint())
				continue
			}
			go dpo.addToCurrent(msg.Transaction, false)
		case <-dpo.stopChan:
			return
		}
	}
}

func (dpo *DPO) setDeliveryManager() {
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
				if _, loaded := dpo.Current.LoadAndDelete(string(tx)); loaded { //TODO: deal with current counter and lazy broadcast
					dpo.pendingToBeBroadcasted_locker.Lock()
					dpo.pendingToBeBroadcasted.Remove(t)
					dpo.pendingToBeBroadcasted_locker.Unlock()
					atomic.AddInt64(&dpo.currentCounter, -1)
				} else {
					atomic.AddInt64(&dpo.totalElementsAdded, 1)
					atomic.AddInt64(&dpo.totalAddedByEpochInc, 1)
				}
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
			if dpo.broadcastType == types.BTAfterEpoch {
				dpo.broadcastSetByOldestElement()
			}
			if dpo.automaticEpochInc {
				go dpo.EpochInc(dpo.nextEpoch)
			}
		case <-dpo.stopChan:
			return
		}
	}
}

func (dpo *DPO) setAddManager() {
	for {
		select {
		case msg := <-dpo.setAddMsgQueue:
			for tx := range msg.TransactionSet {
				t := types.Transaction(tx)
				if !dpo.verificationFunc(t) {
					dpo.logger.Verbose.Printf("Discarded invalid transaction %s\n", msg.Transaction.Hash().Fingerprint())
					continue
				}
				go dpo.addToCurrent(t, false)
			}
		case <-dpo.stopChan:
			return
		}
	}
}

func (dpo *DPO) addToCurrent(tx types.Transaction, toBroadcast bool) (bool, error) {
	_, loaded := dpo.Current.LoadOrStore(string(tx), true)
	if !loaded {
		atomic.AddInt64(&dpo.currentCounter, 1)
		atomic.AddInt64(&dpo.totalElementsAdded, 1)
		dpo.logger.Verbose.Printf("Added record %s\n", string(tx[96:]))
		// if BML != nil {
		// 	BML.AddedElement(tx)
		// }
		if toBroadcast {
			atomic.AddInt64(&dpo.totalAddedByClient, 1)
			if dpo.broadcastType == types.BTImmediate {
				return true, dpo.rb.Broadcast(NewAddPayload(tx))
			} else {
				dpo.pendingToBeBroadcasted_locker.Lock()
				dpo.pendingToBeBroadcasted.Add(tx)
				dpo.pendingToBeBroadcasted_locker.Unlock()
				return true, nil
			}
		}
	} else if !toBroadcast {
		dpo.pendingToBeBroadcasted_locker.Lock()
		dpo.pendingToBeBroadcasted.Remove(tx)
		dpo.pendingToBeBroadcasted_locker.Unlock()
	}
	return false, nil
}

func (dpo *DPO) broadcastSet() {
	dpo.logger.Debug.Println("Broadcasting ", dpo.pendingToBeBroadcasted.Size(), " transactions")

	dpo.pendingToBeBroadcasted_locker.Lock()
	if dpo.pendingToBeBroadcasted.Size() != 0 {
		dpo.rb.Broadcast(NewSetAddPayload(dpo.pendingToBeBroadcasted.GetTransactionSet()))
		dpo.totalSetBroadcasted += int64(dpo.pendingToBeBroadcasted.Size())
		dpo.pendingToBeBroadcasted = types.NewSetToBroadcast(dpo.broadcastType)
	}
	dpo.pendingToBeBroadcasted_locker.Unlock()
}

func (dpo *DPO) GetNextEpoch() types.Epoch {
	dpo.epochChangeLock.RLock()
	defer dpo.epochChangeLock.RUnlock()
	return dpo.nextEpoch
}

func (dpo *DPO) GetProposal(r types.RBBCRound) (*types.Proposal, *time.Time) {
	timeout := time.Now().Add(dpo.epochTimeout)
	propSet := make([]types.Transaction, 0)
	dpo.Current.Range(func(k, v interface{}) bool {
		propSet = append(propSet, types.Transaction(k.(string)))
		return true
	})
	p := types.NewProposal(
		propSet,
		dpo.proposerIdentity,
		types.RBBCRound(dpo.GetNextEpoch()),
	)
	dpo.logger.Debug.Printf("Generated proposal %s for round %d\n", p.Hash.Fingerprint(), r)
	return p, &timeout
}

func (dpo *DPO) periodicBroadcastSet() {
	for {
		select {
		case <-dpo.stopChan:
			return
		default:
			time.Sleep(dpo.broadcastPeriod)
			go dpo.broadcastSet()
		}
	}
}

func (dpo *DPO) broadcastSetByOldestElement() {
	dpo.pendingToBeBroadcasted_locker.Lock()
	oldest := dpo.pendingToBeBroadcasted.GetOldest()
	dpo.pendingToBeBroadcasted_locker.Unlock()
	if oldest.Add(dpo.broadcastPeriod).Before(time.Now()) {
		go dpo.broadcastSet()
	}
}

func (dpo *DPO) ReconciliateFunc(ps []*types.Proposal, currentRound types.RBBCRound) *types.Block {
	block := types.NewBlock(currentRound)
	for _, p := range ps {
		for _, tx := range p.Txs {
			block.Txs.Add(tx)
		}
	}
	return block
}
