package vrb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"time"

	"imdea.org/redbelly/logging"
	rbtypes "imdea.org/redbelly/types"
	"imdea.org/redbelly/vrb/types"
)

const ChannelBuffer = 10000

type MessageLog struct {
	Proposal   *rbtypes.Proposal
	EchoCount  uint
	ReadyCount map[[sha256.Size]byte]uint
	Delivered  bool

	queue       chan *types.Message
	queueClosed bool
	queueLock   sync.Mutex // protects queue

	sentReady     bool
	sentReadyLock sync.Mutex // protects sentReady
}

func NewMessageLog(p *rbtypes.Proposal) *MessageLog {
	return &MessageLog{
		Proposal:   p,
		EchoCount:  0,
		ReadyCount: make(map[[sha256.Size]byte]uint),
		sentReady:  false,
		Delivered:  false,

		queue:       make(chan *types.Message, ChannelBuffer),
		queueClosed: false,

		sentReadyLock: sync.Mutex{},
		queueLock:     sync.Mutex{},
	}
}

func (ml *MessageLog) IncrementReady(verif []rbtypes.TransactionIndex) uint {
	buf := &bytes.Buffer{} //TODO: check if there is a quicker way to do the same
	for _, tx := range verif {
		binary.Write(buf, binary.BigEndian, tx)
	}
	hash := sha256.Sum256(buf.Bytes())
	ml.ReadyCount[hash]++
	return ml.ReadyCount[hash]
}

func (ml *MessageLog) GetAndSetSentReady() bool {
	ml.sentReadyLock.Lock()
	defer ml.sentReadyLock.Unlock()
	if ml.sentReady {
		return true
	} else {
		ml.sentReady = true
		return false
	}
}

func (ml *MessageLog) GetSentReady() bool {
	ml.sentReadyLock.Lock()
	defer ml.sentReadyLock.Unlock()
	return ml.sentReady
}

func (ml *MessageLog) SetSentReady() {
	ml.sentReadyLock.Lock()
	defer ml.sentReadyLock.Unlock()
	ml.sentReady = true
}

func (ml *MessageLog) enqueue(msg *types.Message) {
	ml.queueLock.Lock()
	defer ml.queueLock.Unlock()
	if !ml.queueClosed {
		ml.queue <- msg
	}
}

func (ml *MessageLog) closeQueue() {
	ml.queueLock.Lock()
	defer ml.queueLock.Unlock()
	close(ml.queue)
	ml.queueClosed = true
}

type VerifiedReliableBroadcast struct {
	DeliveryChan chan struct {
		Proposal       *rbtypes.Proposal
		InvalidIndexes []rbtypes.TransactionIndex
	}

	vl types.VerificationLogic
	vd time.Duration

	ns       types.VRBNetworkService
	ownIndex rbtypes.ProposerIndex
	logger   *logging.Logger

	n uint
	t uint

	msgLog sync.Map

	initChan  chan *types.Message
	echoChan  chan *types.Message
	readyChan chan *types.Message

	stopChan chan struct{}
	stopWG   *sync.WaitGroup
}

// NewVerifiedReliableBroadcast creates an instance of VerifiedReliableBroadcast where
// ns is the underlying network service
// pi is the proposer index previously agreed upon by nodes
// n is the total number of nodes and
// t is the Byzantine node tolerance
func NewVerifiedReliableBroadcast(ns types.VRBNetworkService, pi rbtypes.ProposerIndex, n uint, t uint) *VerifiedReliableBroadcast {
	return &VerifiedReliableBroadcast{
		ns:       ns,
		ownIndex: pi,
		logger:   logging.NewLogger("VRB"),

		n: n,
		t: t,

		msgLog: sync.Map{}, //TODO free memory for delivered proposals

		DeliveryChan: make(chan struct {
			Proposal       *rbtypes.Proposal
			InvalidIndexes []rbtypes.TransactionIndex
		}, ChannelBuffer),
		initChan:  make(chan *types.Message, ChannelBuffer),
		echoChan:  make(chan *types.Message, ChannelBuffer),
		readyChan: make(chan *types.Message, ChannelBuffer),

		stopChan: make(chan struct{}, 1),
		stopWG:   &sync.WaitGroup{},
	}
}

func (vrb *VerifiedReliableBroadcast) SetVerification(vl types.VerificationLogic, delay time.Duration) {
	//TODO Check that was not started yet
	vrb.vl = vl
	vrb.vd = delay
}

func (vrb *VerifiedReliableBroadcast) Start() {
	go vrb.incomingLoop()
	go vrb.initLoop()
	go vrb.echoLoop()
	go vrb.readyLoop()
	vrb.logger.Debug.Println("Started")
}

func (vrb *VerifiedReliableBroadcast) Stop() {
	vrb.stopChan <- struct{}{}
	vrb.stopWG.Add(1)
	vrb.stopWG.Wait()
	close(vrb.initChan)
	close(vrb.echoChan)
	close(vrb.readyChan)
	vrb.logger.Debug.Println("Stopped")
}

func (vrb *VerifiedReliableBroadcast) Broadcast(p *rbtypes.Proposal) {
	vrb.ns.VRBBroadcast(types.NewInitMessage(p, vrb.ownIndex))
}

func (vrb *VerifiedReliableBroadcast) incomingLoop() {
main:
	for {
		select {
		case <-vrb.stopChan:
			break main
		case msg := <-vrb.ns.VRBIncoming():
			switch msg.Type {
			case types.Init:
				vrb.initChan <- msg
			case types.Echo:
				vrb.echoChan <- msg
			case types.Ready:
				vrb.readyChan <- msg
			}
		}
	}
	vrb.stopWG.Done()
}

func (vrb *VerifiedReliableBroadcast) initLoop() {
	for msg := range vrb.initChan {
		vrb.logger.Debug.Printf("%s|INIT received", msg.Proposal.Hash.Fingerprint())
		value, loaded := vrb.msgLog.LoadOrStore(msg.Proposal.Hash, NewMessageLog(msg.Proposal))
		if loaded {
			go func(msg *types.Message, value interface{}) {
				msgLog := value.(*MessageLog)
				if msgLog.Proposal != nil {
					vrb.logger.Verbose.Printf("%s|INIT duplicate discarded", msg.Proposal.Hash.Fingerprint())
					return
				}
				vrb.logger.Verbose.Printf("%s|INIT managing late message", msg.Proposal.Hash.Fingerprint())
				msgLog.Proposal = msg.Proposal
				for enqueued := range msgLog.queue {
					vrb.logger.Verbose.Printf("%s|INIT dequeuing", enqueued.ProposalDigest.Fingerprint())
					if enqueued.Type == types.Echo {
						vrb.echoChan <- enqueued
					} else if enqueued.Type == types.Ready {
						vrb.readyChan <- enqueued
					}
				}
			}(msg, value)
		}
		vrb.ns.VRBBroadcast(types.NewEchoMessage(&msg.Proposal.Hash, msg.ProposerIndex))
	}
}

func (vrb *VerifiedReliableBroadcast) echoLoop() {
	for msg := range vrb.echoChan {
		vrb.logger.Verbose.Printf("%s|ECHO received", msg.ProposalDigest.Fingerprint())

		value, found := vrb.msgLog.LoadOrStore(*msg.ProposalDigest, NewMessageLog(nil))
		if !found || value.(*MessageLog).Proposal == nil {
			vrb.logger.Verbose.Printf("%s|ECHO enqueued", msg.ProposalDigest.Fingerprint())
			value.(*MessageLog).enqueue(msg)
			continue
		}
		msgLog := value.(*MessageLog)
		msgLog.EchoCount++
		if msgLog.EchoCount == (vrb.n-vrb.t) && !msgLog.GetSentReady() {
			vrb.logger.Verbose.Printf("%s|READY Sent from echo. Echo count: %d\n", msg.ProposalDigest.Fingerprint(), msgLog.EchoCount)
			if vrb.vl == nil { // Broadcast is not verified so always broadcast
				msgLog.SetSentReady()
				vrb.ns.VRBBroadcast(types.NewReadyMessage(msg.ProposalDigest, msg.ProposerIndex))
			} else {
				go func(msgLog *MessageLog, msg *types.Message) {
					role := vrb.vl.VerifyRole(msgLog.Proposal, vrb.ownIndex)
					vrb.logger.Verbose.Printf("%s|ECHO role: %v\n", msg.ProposalDigest.Fingerprint(), role)
					switch role {
					case types.Primary:
						ready := types.NewReadyMessage(msg.ProposalDigest, msg.ProposerIndex)
						ready.ProposalInvalidIndexes = vrb.vl.VerifyProposal(msgLog.Proposal)
						msgLog.SetSentReady()
						vrb.ns.VRBBroadcast(ready)
					case types.Secondary:
						time.Sleep(vrb.vd)
						if !msgLog.GetAndSetSentReady() {
							ready := types.NewReadyMessage(msg.ProposalDigest, msg.ProposerIndex)
							ready.ProposalInvalidIndexes = vrb.vl.VerifyProposal(msgLog.Proposal)
							vrb.ns.VRBBroadcast(ready)
						}
					}
				}(msgLog, msg)
			}
		}
		vrb.logger.Debug.Printf("%s|ECHO Managed. Echo count: %d\n", msg.ProposalDigest.Fingerprint(), msgLog.EchoCount)
	}
}

func (vrb *VerifiedReliableBroadcast) readyLoop() {
	for msg := range vrb.readyChan { //TODO: Check message correctness
		value, found := vrb.msgLog.LoadOrStore(*msg.ProposalDigest, NewMessageLog(nil))
		if !found || value.(*MessageLog).Proposal == nil {
			vrb.logger.Verbose.Printf("%s|READY enqueued", msg.ProposalDigest.Fingerprint())
			value.(*MessageLog).enqueue(msg)
			continue
		}
		msgLog := value.(*MessageLog)
		readyCount := msgLog.IncrementReady(msg.ProposalInvalidIndexes)
		vrb.logger.Debug.Printf("%s|READY Managed. Ready count: %d\n", msg.ProposalDigest.Fingerprint(), readyCount)
		if readyCount == vrb.t+1 && !msgLog.GetAndSetSentReady() {
			//the set ready also prevent unecessary verifications by secondary verifiers
			vrb.logger.Verbose.Printf("%s|READY Sent from ready\n", msg.ProposalDigest.Fingerprint())
			vrb.ns.VRBBroadcast(msg)
			continue
		}
		if !msgLog.Delivered && readyCount == vrb.n-vrb.t {
			vrb.logger.Debug.Printf("%s|Delivered message\n", msgLog.Proposal.Hash.Fingerprint())
			vrb.DeliveryChan <- struct {
				Proposal       *rbtypes.Proposal
				InvalidIndexes []rbtypes.TransactionIndex
			}{Proposal: msgLog.Proposal, InvalidIndexes: msg.ProposalInvalidIndexes}
			msgLog.Delivered = true
			msgLog.closeQueue()
		}
	}
}
