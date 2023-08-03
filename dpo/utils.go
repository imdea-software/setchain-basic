package dpo

import (
	"bytes"
	"encoding/gob"

	"imdea.org/redbelly/types"

	rbt "imdea.org/redbelly/rb/types"
)

type MessageType uint8

const (
	MTEpochInc MessageType = iota
	MTAdd
	MTSetAdd
)

type Message struct {
	Type           MessageType
	Epoch          types.Epoch
	Transaction    types.Transaction
	TransactionSet types.TransactionSet
}

func NewAddPayload(tx types.Transaction) rbt.Payload {
	buf := &bytes.Buffer{}
	encoder := gob.NewEncoder(buf)
	encoder.Encode(&Message{
		Type:        MTAdd,
		Transaction: tx,
	})
	return buf.Bytes()
}

func NewSetAddPayload(ts types.TransactionSet) rbt.Payload {
	buf := &bytes.Buffer{}
	encoder := gob.NewEncoder(buf)
	encoder.Encode(&Message{
		Type:           MTSetAdd,
		TransactionSet: ts,
	})
	return buf.Bytes()
}

func NewEpochIncPayload(e types.Epoch) rbt.Payload {
	buf := &bytes.Buffer{}
	encoder := gob.NewEncoder(buf)
	encoder.Encode(&Message{
		Type:  MTEpochInc,
		Epoch: e,
	})
	return buf.Bytes()
}

func DecodePayload(p rbt.Payload) *Message {
	buf := bytes.NewBuffer(p)
	decoder := gob.NewDecoder(buf)
	m := &Message{}
	decoder.Decode(m)
	return m
}
