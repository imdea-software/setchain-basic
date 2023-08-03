package bc

import (
	"imdea.org/redbelly/types"
)

type MessageType uint8

const (
	Est MessageType = iota
	Coord
	Aux
)

type Round int
type Value int

func NewValue(v bool) Value {
	if v {
		return 1
	} else {
		return 0
	}
}

func (v Value) GetBool() bool {
	if v == 0 {
		return false
	} else {
		return true
	}
}

type ValueSet map[Value]struct{}

func (s ValueSet) contains(value Value) bool {
	_, c := s[value]
	return c
}

func (s ValueSet) getValue() (v Value, _ bool) {
	for v = range s {
		return v, true
	}
	return v, false
}

func (s ValueSet) insert(v Value) {
	s[v] = struct{}{}
}

func singleton(v Value) ValueSet {
	s := make(ValueSet)
	s[v] = struct{}{}
	return s
}

type ProposerIndexSet map[types.ProposerIndex]struct{}

func (s ProposerIndexSet) Add(idx types.ProposerIndex) {
	s[idx] = struct{}{}
}

func (s1 ProposerIndexSet) Union(s2 ProposerIndexSet) {
	for idx := range s2 {
		s1.Add(idx)
	}
}

type Message struct {
	Type               MessageType
	ProposalIdentifier types.ProposalIdentifier
	Round              Round
	Value              Value
	ValueSet           ValueSet
	Sender             types.ProposerIndex
}

func NewEstMessage(pi types.ProposalIdentifier, r Round, v Value, ps types.ProposerIndex) *Message {
	return &Message{
		Type:               Est,
		ProposalIdentifier: pi,
		Round:              r,
		Value:              v,
		Sender:             ps,
	}
}

func NewCoordMessage(pi types.ProposalIdentifier, r Round, v Value) *Message {
	return &Message{
		Type:               Coord,
		ProposalIdentifier: pi,
		Round:              r,
		Value:              v,
	}
}

func NewAuxMessage(pi types.ProposalIdentifier, r Round, s ValueSet, ps types.ProposerIndex) *Message {
	return &Message{
		Type:               Aux,
		ProposalIdentifier: pi,
		Round:              r,
		ValueSet:           s,
		Sender:             ps,
	}
}

type MsgIdentifier struct {
	ProposalIdentifier types.ProposalIdentifier
	Round              Round
	Value              Value
}

func NewMsgIdentifier(pi types.ProposalIdentifier, r Round, v Value) *MsgIdentifier {
	return &MsgIdentifier{
		ProposalIdentifier: pi,
		Round:              r,
		Value:              v,
	}
}

type BCNetworkService interface {
	BCBroadcast(*Message)
	BCIncoming() <-chan *Message
}
