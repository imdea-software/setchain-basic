package p2p

import (
	"imdea.org/redbelly/bc"
	reltypes "imdea.org/redbelly/rb/types"
	vrbtypes "imdea.org/redbelly/vrb/types"
)

type PacketType uint8

const (
	VRB = iota
	BIN
	RB
)

type Packet struct {
	Type       PacketType
	VRBPayload *vrbtypes.Message
	BCPayload  *bc.Message
	RBPayload  *reltypes.Message
}

func NewVRBPacket(p *vrbtypes.Message) *Packet {
	return &Packet{
		Type:       VRB,
		VRBPayload: p,
	}
}

func NewBCPacket(p *bc.Message) *Packet {
	return &Packet{
		Type:      BIN,
		BCPayload: p,
	}
}

func NewRBPacket(p *reltypes.Message) *Packet {
	return &Packet{
		Type:      RB,
		RBPayload: p,
	}
}

type NetworkService interface {
	Broadcast(*Packet) error
	Incoming() <-chan *Packet
}
