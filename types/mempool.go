package types

import (
	"sync"
)

type Mempool interface {
	Push(Transaction) error
	Get() []Transaction
	Clean([]Transaction)
	Len() int
	In(Transaction) bool
}

// LocalMempool
//TODO: limit mempool capacity
type LocalMempool struct {
	txs []struct {
		tx   Transaction
		hash Hash
	}
	hashMap map[Hash]bool
	lock    *sync.Mutex
}

func NewLocalMempool() *LocalMempool {
	return &LocalMempool{
		txs: make([]struct {
			tx   Transaction
			hash Hash
		}, 0),
		hashMap: map[Hash]bool{},
		lock:    &sync.Mutex{},
	}
}

func (m *LocalMempool) Push(tx Transaction) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.txs = append(m.txs, struct {
		tx   Transaction
		hash Hash
	}{tx: tx, hash: tx.Hash()})
	m.hashMap[m.txs[len(m.txs)-1].hash] = true
	return nil //TODO: return error if capacity exceeded
}

func (m *LocalMempool) Get() []Transaction {
	m.lock.Lock()
	defer m.lock.Unlock()
	txs := make([]Transaction, len(m.txs))
	for i, v := range m.txs {
		txs[i] = v.tx
	}
	return txs
}

func (m *LocalMempool) Clean(txs []Transaction) {
	m.lock.Lock()
	defer m.lock.Unlock()
	toCleanHashes := make(map[Hash]bool)
	for _, tx := range txs {
		if m.hashMap[tx.Hash()] {
			toCleanHashes[tx.Hash()] = true
		}
	}
	for i := 0; i < len(m.txs); i++ {
		if _, ok := toCleanHashes[m.txs[i].hash]; ok { //TODO: delete efficiently preserving original order
			delete(m.hashMap, m.txs[i].hash)
			m.txs[i] = m.txs[len(m.txs)-1]
			m.txs = m.txs[:len(m.txs)-1]
			i--
		}
	}
}

func (m *LocalMempool) Len() int {
	m.lock.Lock()
	defer m.lock.Unlock()
	return len(m.txs)
}

func (m *LocalMempool) In(tx Transaction) bool {
	return m.hashMap[tx.Hash()]
}
