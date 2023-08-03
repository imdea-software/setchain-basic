package p2p

import (
	"imdea.org/redbelly/bc"

	reltypes "imdea.org/redbelly/rb/types"
	vrbtypes "imdea.org/redbelly/vrb/types"
)

const ChannelBuffer = 10000

type Router struct {
	ns          NetworkService
	vrbIncoming chan *vrbtypes.Message
	bcIncoming  chan *bc.Message
	rbIncoming  chan *reltypes.Message
	quit        chan struct{}
}

func NewRouter(ns NetworkService) *Router {
	return &Router{
		ns:          ns,
		vrbIncoming: make(chan *vrbtypes.Message, ChannelBuffer),
		bcIncoming:  make(chan *bc.Message, ChannelBuffer),
		rbIncoming:  make(chan *reltypes.Message, ChannelBuffer),
		quit:        make(chan struct{}, 1),
	}
}

func (r *Router) Start() {
	go r.incomingLoop()
}

func (r *Router) Stop() {
	r.quit <- struct{}{}
}

func (r *Router) VRBBroadcast(msg *vrbtypes.Message) {
	r.ns.Broadcast(NewVRBPacket(msg))
}

func (r *Router) VRBIncoming() <-chan *vrbtypes.Message {
	return r.vrbIncoming
}

func (r *Router) BCBroadcast(msg *bc.Message) {
	r.ns.Broadcast(NewBCPacket(msg))
}

func (r *Router) BCIncoming() <-chan *bc.Message {
	return r.bcIncoming
}

func (r *Router) RBBroadcast(msg *reltypes.Message) {
	r.ns.Broadcast(NewRBPacket(msg))
}

func (r *Router) RBIncoming() <-chan *reltypes.Message {
	return r.rbIncoming
}

func (r *Router) incomingLoop() {
main:
	for {
		select {
		case p := <-r.ns.Incoming():
			switch p.Type {
			case VRB:
				r.vrbIncoming <- p.VRBPayload
			case BIN:
				r.bcIncoming <- p.BCPayload
			case RB:
				r.rbIncoming <- p.RBPayload
			}
		case <-r.quit:
			break main
		}
	}
}
