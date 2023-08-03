package rb

import (
	"fmt"
	"sync"

	"imdea.org/redbelly/logging"
	t "imdea.org/redbelly/rb/types"
	rbt "imdea.org/redbelly/types"
)

type MessageLog struct {
	Payload     t.Payload
	PayloadHash rbt.Hash
	EchoCount   uint
	ReadyCount  uint
	Delivered   bool
	logger      *logging.Logger

	queue       chan *t.Message
	queueClosed bool
	queueLock   sync.Mutex // protects queue

	sentReady     bool
	sentReadyLock sync.Mutex // protects sentReady
}

func NewMessageLog() *MessageLog {
	return &MessageLog{
		EchoCount:  0,
		ReadyCount: 0,
		sentReady:  false,
		Delivered:  false,

		queue:       make(chan *t.Message, ChannelBuffer),
		queueClosed: false,

		sentReadyLock: sync.Mutex{},
		queueLock:     sync.Mutex{},
	}
}

func NewMessageLogFromHash(ph rbt.Hash) *MessageLog {
	ml := NewMessageLog()
	ml.PayloadHash = ph
	ml.logger = logging.NewLogger(fmt.Sprintf("RB|%s", ml.PayloadHash.Fingerprint()))
	return ml
}

func NewMessageLogFromPayload(p t.Payload) *MessageLog {
	ml := NewMessageLogFromHash(p.Hash())
	ml.Payload = p
	return ml
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

func (ml *MessageLog) InitMessage() *t.Message {
	return &t.Message{
		Type:    t.Init,
		Payload: ml.Payload,
	}
}

func (ml *MessageLog) EchoMessage() *t.Message {
	return &t.Message{
		Type:        t.Echo,
		PayloadHash: ml.PayloadHash,
	}
}

func (ml *MessageLog) ReadyMessage() *t.Message {
	return &t.Message{
		Type:        t.Ready,
		PayloadHash: ml.PayloadHash,
	}
}

func (ml *MessageLog) enqueue(msg *t.Message) {
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
