package types

import (
	"crypto/sha256"

	"imdea.org/redbelly/types"
)

type Payload []byte

func (p Payload) Hash() types.Hash {
	return sha256.Sum256(p)
}

type RBNetworkService interface {
	RBBroadcast(*Message)
	RBIncoming() <-chan *Message
}

type MessageType uint8

const (
	Init MessageType = iota
	Echo
	Ready
)

type Message struct {
	Type        MessageType
	Payload     Payload
	PayloadHash types.Hash
}

type IReliableBroadcast interface {
	Broadcast(Payload) error
	DeliveryChan() <-chan Payload
}
