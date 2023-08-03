package rb

import (
	"bytes"
	"context"
	"encoding/gob"
	"time"

	"imdea.org/redbelly/rb/types"
)

// custom encoding puts
func NewAggregatedPayload(payloads []types.Payload) types.Payload {
	buf := &bytes.Buffer{}
	encoder := gob.NewEncoder(buf)
	encoder.Encode(payloads)
	return buf.Bytes()
}

func SplitAggregatedPayloads(p types.Payload) []types.Payload {
	buf := bytes.NewBuffer(p)
	decoder := gob.NewDecoder(buf)
	payloads := make([]types.Payload, 0)
	decoder.Decode(&payloads)
	return payloads
}

type AggregatedReliableBroadcast struct {
	aggregation int
	period      time.Duration

	brd types.IReliableBroadcast

	broadcastChan chan types.Payload
	deliveryChan  chan types.Payload

	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewAggregatedReliableBroadcast(maxAggregation int, period time.Duration, brd types.IReliableBroadcast) *AggregatedReliableBroadcast {
	return &AggregatedReliableBroadcast{
		aggregation: maxAggregation,
		period:      period,

		brd: brd,

		broadcastChan: make(chan types.Payload, ChannelBuffer),
		deliveryChan:  make(chan types.Payload, ChannelBuffer),
	}
}

func (arb *AggregatedReliableBroadcast) Start() {
	arb.ctx, arb.cancelFunc = context.WithCancel(context.TODO())
	brdCtx, _ := context.WithCancel(arb.ctx)
	go arb.broadcaster(arb.period, brdCtx)
	deliverCtx, _ := context.WithCancel(arb.ctx)
	go arb.deliverer(deliverCtx)
}

func (arb *AggregatedReliableBroadcast) Stop() {
	arb.cancelFunc()
}

func (arb *AggregatedReliableBroadcast) Broadcast(p types.Payload) error {
	arb.broadcastChan <- p
	return nil
}

func (arb *AggregatedReliableBroadcast) DeliveryChan() <-chan types.Payload {
	return arb.deliveryChan
}

func (arb *AggregatedReliableBroadcast) broadcaster(period time.Duration, ctx context.Context) {
	ticker := time.NewTicker(period)
	payloadBuffer := make([]types.Payload, 0, arb.aggregation)
broadcastLoop:
	for {
		select {
		case p := <-arb.broadcastChan:
			payloadBuffer = append(payloadBuffer, p) //consider having an external counter for performance
			if len(payloadBuffer) == int(arb.aggregation) {
				arb.brd.Broadcast(NewAggregatedPayload(payloadBuffer))
				payloadBuffer = payloadBuffer[:0]
				ticker.Reset(period)
			}
		case <-ticker.C:
			arb.brd.Broadcast(NewAggregatedPayload(payloadBuffer))
			payloadBuffer = payloadBuffer[:0]
		case <-ctx.Done():
			break broadcastLoop
		}
	}
}

func (arb *AggregatedReliableBroadcast) deliverer(ctx context.Context) {
	replayCheck := make(map[string]bool)
deliverLoop:
	for {
		select {
		case aggr := <-arb.brd.DeliveryChan():
			for _, p := range SplitAggregatedPayloads(aggr) {
				if !replayCheck[string(p)] {
					arb.deliveryChan <- p
					replayCheck[string(p)] = true
				}
			}
		case <-ctx.Done():
			break deliverLoop
		}
	}
}
