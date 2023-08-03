package rb

import (
	"errors"
	"sync"

	"imdea.org/redbelly/logging"
	t "imdea.org/redbelly/rb/types"
)

const ChannelBuffer = 10000

type ReliableBroadcast struct {
	deliveryChan chan t.Payload

	ns     t.RBNetworkService
	logger *logging.Logger

	n uint
	t uint

	msgLog sync.Map

	initChan  chan *t.Message
	echoChan  chan *t.Message
	readyChan chan *t.Message

	stopChan chan struct{}
	stopWG   *sync.WaitGroup
}

// NewReliableBroadcast creates an instance of ReliableBroadcast where
// ns is the underlying network service
// pi is the proposer index previously agreed upon by nodes
// n is the total number of nodes and
// t is the Byzantine node tolerance
func NewReliableBroadcast(ns t.RBNetworkService, n uint, f uint) *ReliableBroadcast {
	return &ReliableBroadcast{
		ns:     ns,
		logger: logging.NewLogger("RB"),

		n: n,
		t: f,

		msgLog: sync.Map{}, //TODO free memory for delivered proposals

		deliveryChan: make(chan t.Payload, ChannelBuffer),
		initChan:     make(chan *t.Message, ChannelBuffer),
		echoChan:     make(chan *t.Message, ChannelBuffer),
		readyChan:    make(chan *t.Message, ChannelBuffer),

		stopChan: make(chan struct{}, 1),
		stopWG:   &sync.WaitGroup{},
	}
}

func (rb *ReliableBroadcast) Start() {
	go rb.incomingLoop()
	go rb.initLoop()
	go rb.echoLoop()
	go rb.readyLoop()
	rb.logger.Debug.Println("Started")
}

func (rb *ReliableBroadcast) Stop() {
	rb.stopWG.Add(1)
	rb.stopChan <- struct{}{}
	rb.stopWG.Wait()
	close(rb.initChan)
	close(rb.echoChan)
	close(rb.readyChan)
	rb.logger.Debug.Println("Stopped")
}

func (rb *ReliableBroadcast) Broadcast(p t.Payload) error {
	if v, loaded := rb.msgLog.LoadOrStore(p.Hash(), NewMessageLogFromPayload(p)); loaded {
		return errors.New("payload already broadcasted")
	} else {
		rb.ns.RBBroadcast(v.(*MessageLog).InitMessage())
		return nil
	}
}

func (rb *ReliableBroadcast) DeliveryChan() <-chan t.Payload {
	return rb.deliveryChan
}

func (rb *ReliableBroadcast) incomingLoop() {
main:
	for {
		select {
		case <-rb.stopChan:
			break main
		case msg := <-rb.ns.RBIncoming():
			switch msg.Type {
			case t.Init:
				rb.initChan <- msg
			case t.Echo:
				rb.echoChan <- msg
			case t.Ready:
				rb.readyChan <- msg
			}
		}
	}
	rb.stopWG.Done()
}

func (rb *ReliableBroadcast) initLoop() {
	for msg := range rb.initChan {
		value, loaded := rb.msgLog.LoadOrStore(msg.Payload.Hash(), NewMessageLogFromPayload(msg.Payload))
		msgLog := value.(*MessageLog)
		msgLog.logger.Debug.Println("INIT received")
		if loaded && msgLog.Payload == nil {
			go func(msg *t.Message, msgLog *MessageLog) {
				msgLog.logger.Verbose.Println("INIT managing late message")
				msgLog.Payload = msg.Payload
				for enqueued := range msgLog.queue {
					msgLog.logger.Verbose.Println("INIT dequeuing")
					if enqueued.Type == t.Echo {
						rb.echoChan <- enqueued
					} else if enqueued.Type == t.Ready {
						rb.readyChan <- enqueued
					}
				}
			}(msg, msgLog)
		}
		rb.ns.RBBroadcast(msgLog.EchoMessage())
	}
}

func (rb *ReliableBroadcast) echoLoop() {
	for msg := range rb.echoChan {
		value, found := rb.msgLog.LoadOrStore(msg.PayloadHash, NewMessageLogFromHash(msg.PayloadHash))
		msgLog := value.(*MessageLog)
		if !found || msgLog.Payload == nil {
			msgLog.logger.Verbose.Println("ECHO enqueued")
			msgLog.enqueue(msg)
			continue
		}
		msgLog.EchoCount++
		if msgLog.EchoCount == (rb.n-rb.t) && !msgLog.GetAndSetSentReady() {
			msgLog.logger.Verbose.Printf("READY Sent from echo. Echo count: %d\n", msgLog.EchoCount)
			rb.ns.RBBroadcast(msgLog.ReadyMessage())
		}
		msgLog.logger.Debug.Printf("ECHO Managed. Echo count: %d\n", msgLog.EchoCount)
	}
}

func (rb *ReliableBroadcast) readyLoop() {
	for msg := range rb.readyChan { //TODO: Check message correctness
		value, found := rb.msgLog.LoadOrStore(msg.PayloadHash, NewMessageLogFromHash(msg.PayloadHash))
		msgLog := value.(*MessageLog)
		if !found || msgLog.Payload == nil {
			msgLog.logger.Verbose.Println("READY enqueued")
			msgLog.enqueue(msg)
			continue
		}
		msgLog.ReadyCount++ //non-TS since it is incremented and read sequentially in readyLoop only
		msgLog.logger.Debug.Printf("READY Managed. Ready count: %d\n", msgLog.ReadyCount)
		if msgLog.ReadyCount == rb.t+1 && !msgLog.GetAndSetSentReady() {
			msgLog.logger.Verbose.Println("READY Sent from ready")
			rb.ns.RBBroadcast(msg)
			continue
		}
		if !msgLog.Delivered && msgLog.ReadyCount == rb.n-rb.t {
			rb.logger.Debug.Printf("%s|Delivered message\n", msgLog.PayloadHash.Fingerprint())
			rb.deliveryChan <- msgLog.Payload
			msgLog.Delivered = true
			msgLog.closeQueue()
		}
	}
}
