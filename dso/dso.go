package dso

import (
	"context"
	"sync"

	relbrotypes "imdea.org/redbelly/rb/types"
	"imdea.org/redbelly/types"
)

type IDSO interface {
	Add(r types.Transaction) error
	Get() (types.TransactionSet, error)
}

type DSO struct {
	Set *sync.Map
	rb  relbrotypes.IReliableBroadcast

	addWaitGroups *sync.Map

	cancel context.CancelFunc
}

func NewDSO(rb relbrotypes.IReliableBroadcast) *DSO {
	return &DSO{
		Set: &sync.Map{},
		rb:  rb,

		addWaitGroups: &sync.Map{},
	}
}

func (dso *DSO) Add(r types.Transaction) {
	if _, in := dso.Set.Load(string(r)); in {
		return
	}
	dso.rb.Broadcast(relbrotypes.Payload(r))
	// wch := make(chan interface{})
	// dso.addWaitGroups.Store(string(r), wch)
	// <-wch
}

func (dso *DSO) Get() types.TransactionSet {
	response := types.TransactionSet{}
	dso.Set.Range(func(key, value interface{}) bool {
		response.Add(types.Transaction(key.(string)))
		return true
	})
	return response
}

func (dso *DSO) Start() {
	var deliveryCtx context.Context
	deliveryCtx, dso.cancel = context.WithCancel(context.Background())
	go dso.brdDeliveryManager(deliveryCtx)
}

func (dso *DSO) Stop() {
	dso.cancel()
}

func (dso *DSO) brdDeliveryManager(ctx context.Context) {
deliveryLoop:
	for {
		select {
		case p := <-dso.rb.DeliveryChan():
			dso.Set.Store(string(p), true)
			// if wch, loaded := dso.addWaitGroups.Load(string(p)); loaded {
			// 	wch.(chan interface{}) <- struct{}{}
			// }
		case <-ctx.Done():
			break deliveryLoop
		}
	}
}
