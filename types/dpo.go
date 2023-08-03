package types

import "time"

type Epoch uint64

type TransactionSet map[string]bool

func NewTransactionSet(txs []Transaction) TransactionSet {
	set := TransactionSet{}
	for _, tx := range txs {
		set[string(tx)] = true
	}
	return set
}

func (ts TransactionSet) In(tx Transaction) bool {
	return ts[string(tx)]
}

func (ts TransactionSet) Add(tx Transaction) {
	ts[string(tx)] = true
}

func (ts TransactionSet) Remove(tx Transaction) {
	delete(ts, string(tx))
}

func (ts TransactionSet) IsEqual(txs TransactionSet) bool {
	if len(ts) != len(txs) {
		return false
	}
	for k := range ts {
		if !txs[k] {
			return false
		}
	}
	return true
}

func (ts TransactionSet) Union(txs TransactionSet) {
	for tx := range txs {
		ts[tx] = true
	}
}

func (ts TransactionSet) Slice() []Transaction {
	txs := make([]Transaction, 0, len(ts))
	for tx := range ts {
		txs = append(txs, Transaction(tx))
	}
	return txs
}

func (ts TransactionSet) Size() int {
	return len(ts)
}

func (ts TransactionSet) GetOldest() time.Time {
	return time.Now()
}

func (ts TransactionSet) GetTransactionSet() TransactionSet {
	return ts
}

type History map[Epoch]TransactionSet

type TransactionSetTimestamped map[string]time.Time

func (ts TransactionSetTimestamped) Add(tx Transaction) {
	ts[string(tx)] = time.Now()
}

func (ts TransactionSetTimestamped) Remove(tx Transaction) {
	delete(ts, string(tx))
}

func (ts TransactionSetTimestamped) GetOldest() time.Time {
	res := time.Now()
	for _, t := range ts {
		if t.Before(res) {
			res = t
		}
	}
	return res
}

func (ts TransactionSetTimestamped) GetTransactionSet() TransactionSet {
	set := TransactionSet{}
	for tx := range ts {
		set[tx] = true
	}
	return set
}

func (ts TransactionSetTimestamped) Size() int {
	return len(ts)
}

type SetToBroadcast interface {
	Add(tx Transaction)
	Remove(tx Transaction)
	GetOldest() time.Time
	GetTransactionSet() TransactionSet
	Size() int
}

type BroadcastType uint8

const (
	BTImmediate BroadcastType = iota
	BTPeriodic
	BTAfterEpoch
)

func NewSetToBroadcast(bt BroadcastType) SetToBroadcast {
	if bt == BTAfterEpoch {
		return TransactionSetTimestamped{}
	}
	return TransactionSet{}
}
