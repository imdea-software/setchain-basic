package simulator

import (
	"bytes"
	"encoding/gob"

	"imdea.org/redbelly/logging"
	"imdea.org/redbelly/p2p"
)

const PacketBuffer = 100

var logger = logging.NewLogger("SIM")

type NetworkServiceSimulator struct {
	broadcastChannel chan<- *p2p.Packet
	incomingChannel  <-chan *p2p.Packet
}

func (nss *NetworkServiceSimulator) Broadcast(p *p2p.Packet) error {
	nss.broadcastChannel <- p
	return nil
}

func (nss *NetworkServiceSimulator) Incoming() <-chan *p2p.Packet {
	return nss.incomingChannel
}

type Simulator struct {
	delay uint

	broadcastChannel chan *p2p.Packet
	incomingChannels []chan *p2p.Packet
	quit             chan struct{}
}

func NewSimulator(delay uint) *Simulator {
	logger.Warning.Println("Delay is not implemented !")
	return &Simulator{
		delay:            delay,
		broadcastChannel: make(chan *p2p.Packet, PacketBuffer),
		incomingChannels: make([]chan *p2p.Packet, 0),
		quit:             make(chan struct{}, 1),
	}
}

func (s *Simulator) Start() {
	go s.broadcaster()
}

func (s *Simulator) Stop() {
	s.quit <- struct{}{}
}

//TODO: Different node should normally expereince different constant delays
func (s *Simulator) Broadcast(p *p2p.Packet) {
	s.broadcastChannel <- p
}

func (s *Simulator) NewNode() (*NetworkServiceSimulator, int) {
	s.incomingChannels = append(s.incomingChannels, make(chan *p2p.Packet, PacketBuffer))
	return &NetworkServiceSimulator{
		incomingChannel:  s.incomingChannels[len(s.incomingChannels)-1],
		broadcastChannel: s.broadcastChannel,
	}, len(s.incomingChannels) - 1
}

func (s *Simulator) broadcaster() {
	var err error
	fakeNetwork := &bytes.Buffer{}
	enc := gob.NewEncoder(fakeNetwork)
	dec := gob.NewDecoder(fakeNetwork)
main:
	for {
		select {
		case p := <-s.broadcastChannel:
			for _, ch := range s.incomingChannels {
				err = enc.Encode(p)
				if err != nil {
					logger.Error.Println("Error encoding packet")
					continue
				}
				var packet p2p.Packet
				err = dec.Decode(&packet)
				if err != nil {
					logger.Error.Panicln("Error decoding packet")
					continue
				}
				ch <- &packet
			}
		case <-s.quit:
			break main
		}
	}
}
