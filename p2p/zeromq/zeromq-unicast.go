package zeromq

import (
	"bytes"
	"encoding/gob"
	"sync"

	"imdea.org/redbelly/logging"
	"imdea.org/redbelly/p2p"
	"imdea.org/redbelly/types"

	zmq "github.com/pebbe/zmq4"
)

const Ping = "ping"
const ChannelBuffer = 10000

var ConnectionAttempts = 10
var ConnectionAttemptDelay = 2

type ZMQUNetworkService struct {
	nodeID int
	nnodes int
	logger *logging.Logger

	outSockets []*zmq.Socket
	inSocket   *zmq.Socket
	outAddrs   []string
	inAddr     string
	incQueue   chan *p2p.Packet
	stopChan   chan struct{}
	outLocks   []sync.Mutex

	behaviour types.ServerBehaviour
}

func NewZMQUNetworkService(nodeID int, nnodes int, outAddrs []string, inAddr string, behaviour types.ServerBehaviour) *ZMQUNetworkService {
	return &ZMQUNetworkService{
		nodeID: nodeID,
		nnodes: nnodes,
		logger: logging.NewLogger("ZMQU"),

		outSockets: make([]*zmq.Socket, nnodes),
		outAddrs:   outAddrs,
		inAddr:     inAddr,
		incQueue:   make(chan *p2p.Packet, ChannelBuffer),
		stopChan:   make(chan struct{}, 1),
		outLocks:   make([]sync.Mutex, nnodes),

		behaviour: behaviour,
	}
}

func (ns *ZMQUNetworkService) Start() error {
	context, err := zmq.NewContext()
	if err != nil {
		ns.logger.Error.Printf("error trying to create context %s\n", err)
		return err
	}
	err = context.SetMaxSockets(1024)
	if err != nil {
		ns.logger.Error.Println(err)
		return err
	}

	ns.inSocket, err = context.NewSocket(zmq.REP)
	if err != nil {
		ns.logger.Error.Printf("error trying to create in socket %s\n", err)
		return err
	}
	err = ns.inSocket.Bind(ns.inAddr)
	if err != nil {
		ns.logger.Error.Printf("error trying to bind in socket %s\n", err)
		return err
	}

	go ns.incomingLoop()

	for id := 0; id < ns.nnodes; id++ {
		if id == ns.nodeID {
			continue
		}
		ns.outSockets[id], err = context.NewSocket(zmq.REQ)
		if err != nil {
			ns.logger.Error.Printf("error trying to create out socket %d, %s\n", id, err)
			return err
		}
		err = ns.outSockets[id].Connect(ns.outAddrs[id])
		if err != nil {
			ns.logger.Warning.Printf("Error with ZMQ socket connect for %s: %d\n", ns.outAddrs[id], err)
			break
		}
	}
	return nil
}

func (ns *ZMQUNetworkService) Stop() {
	ns.stopChan <- struct{}{}
	for id, s := range ns.outSockets {
		if id == ns.nodeID {
			continue
		}
		s.Close() //TODO: move at the end of sender loop
	}
}

func (ns *ZMQUNetworkService) incomingLoop() {
	ns.logger.Verbose.Printf("Started IncomingLoop\n")
	pingBytes := []byte(Ping)
main:
	for {
		select {
		case <-ns.stopChan:
			ns.logger.Verbose.Printf("Stopped IncomingLoop\n")
			break main
		default: //TODO: if it enters here and the connection is already closed it will fail/wait forever?
			ns.logger.Verbose.Printf("Waiting IncomingLoop\n")

			m, err := ns.inSocket.RecvBytes(0)
			if err != nil {
				ns.logger.Verbose.Printf("ERROR %s IncomingLoop\n", err)
				continue
			}
			if ns.behaviour == types.Silent {
				if bytes.Equal(m, pingBytes) {
					ns.inSocket.Send("", 0)
				}
				continue
			}

			if !bytes.Equal(m, pingBytes) {
				go func(m []byte) {
					buff := bytes.Buffer{}
					decoder := gob.NewDecoder(&buff)
					var msg p2p.Packet
					buff.Write(m)
					err = decoder.Decode(&msg)
					if err != nil {
						ns.logger.Error.Println(err)
					}
					ns.logger.Verbose.Printf("Received %v\n", msg)
					ns.incQueue <- &msg
				}(m)
			}
			_, err = ns.inSocket.Send("", 0)
			if err != nil {
				ns.logger.Info.Println(err)
			}
		}
	}
	ns.inSocket.Close()
}

func (ns *ZMQUNetworkService) Send(id int, msg []byte) error {
	if ns.behaviour == types.Silent {
		return nil
	}

	ns.outLocks[id].Lock() //TODO: Remove lock and implement a send channel with a senderLoop
	defer ns.outLocks[id].Unlock()

	_, err := ns.outSockets[id].SendBytes(msg, 0)
	if err != nil {
		ns.logger.Error.Println(err)
		return err
	}
	ns.logger.Verbose.Printf("Sent message to %d\n", id)

	_, err = ns.outSockets[id].Recv(0)
	if err != nil {
		ns.logger.Error.Println(err)
	}
	return err
}

func (ns *ZMQUNetworkService) Broadcast(p *p2p.Packet) error {
	buff := bytes.Buffer{}
	encoder := gob.NewEncoder(&buff)
	encoder.Encode(p)
	payload := make([]byte, buff.Len())
	buff.Read(payload)
	ns.logger.Verbose.Printf("Sent %v to myself \n", p)

	ns.incQueue <- p //send it to myself
	go func(p []byte) {
		for id := range ns.outSockets {
			if id == ns.nodeID {
				continue
			}
			go ns.Send(id, payload)
		}
	}(payload)
	return nil //TODO: return error if one of the send fails
}

func (ns *ZMQUNetworkService) Incoming() <-chan *p2p.Packet {
	return ns.incQueue
}

func (ns *ZMQUNetworkService) PingAll() <-chan bool {
	signalChannel := make(chan bool, 1)
	go func() {
		for i := 0; i < ns.nnodes; i++ { //If required these can be parallelized
			if i == ns.nodeID {
				continue
			}
			ns.Send(i, []byte(Ping))
		}
		signalChannel <- true
	}() //can avoid passing argument since the calling function terminates after creating goroutine
	return signalChannel
}
