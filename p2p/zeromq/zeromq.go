package zeromq

import (
	"bytes"
	"encoding/gob"
	"sync"

	"imdea.org/redbelly/logging"
	"imdea.org/redbelly/p2p"

	zmq "github.com/pebbe/zmq4"
)

const MessagesBufferSize = 100

type ZMQNetworkService struct {
	endpoint string
	nodeID   string
	logger   *logging.Logger
	pub      *zmq.Socket
	sub      *zmq.Socket
	incQueue chan *p2p.Packet
	stopChan chan struct{}
	pubLock  sync.Mutex
}

func NewZMQNetworkService(endpoint string, nodeID string) *ZMQNetworkService {
	return &ZMQNetworkService{
		endpoint: endpoint,
		nodeID:   nodeID,
		logger:   logging.NewLogger("ZMQ" + nodeID),
		incQueue: make(chan *p2p.Packet, MessagesBufferSize),
		stopChan: make(chan struct{}, 1),
		pubLock:  sync.Mutex{},
	}
}

func (ns *ZMQNetworkService) Start() error {
	var err error
	ns.pub, err = zmq.NewSocket(zmq.PUB)
	if err != nil {
		ns.logger.Error.Printf("error creating pub %s\n", err)

		return err
	}
	err = ns.pub.Bind(ns.endpoint)
	if err != nil {
		ns.logger.Error.Printf("error binding %s\n", err)

		return err
	}
	ns.logger.Debug.Printf("Node %v binded to %v\n", ns.nodeID, ns.endpoint)
	ns.sub, err = zmq.NewSocket(zmq.SUB)
	if err != nil {
		ns.logger.Error.Printf("error creating sub %s\n", err)

		return err
	}

	go ns.incomingLoop()

	return nil
}

func (ns *ZMQNetworkService) Stop() {
	ns.stopChan <- struct{}{}
	ns.pub.Close()
	ns.sub.Close()
}

func (ns *ZMQNetworkService) Connect(endpoint string) error {
	var err error
	err = ns.sub.Connect(endpoint)
	if err != nil {
		ns.logger.Error.Println(err)

		return err
	}
	err = ns.sub.SetSubscribe("") //subscribe to all incoming messages
	if err != nil {

		ns.logger.Error.Printf("error connecting %s\n", err)

		return err
	}
	ns.logger.Debug.Printf("Node %v connected to %v\n", ns.nodeID, endpoint)

	return nil
}

func (ns *ZMQNetworkService) incomingLoop() {
main:
	for {
		select {
		case <-ns.stopChan:
			break main
		default: //TODO: if it enters here and the connection is already closed it will fail/wait forever?
			m, err := ns.sub.RecvBytes(0)
			if err != nil {
				ns.logger.Error.Printf("error receiving %s\n", err)
			}
			go func(m []byte) {
				buff := bytes.Buffer{}
				decoder := gob.NewDecoder(&buff)
				var msg p2p.Packet
				buff.Write(m)
				err = decoder.Decode(&msg)
				if err != nil {
					ns.logger.Error.Printf("error decoding %s\n", err)
				}
				ns.logger.Verbose.Printf("Received %v\n", msg)

				ns.incQueue <- &msg
			}(m)
		}
	}
}

func (ns *ZMQNetworkService) Broadcast(p *p2p.Packet) error {
	buff := bytes.Buffer{}
	encoder := gob.NewEncoder(&buff)
	encoder.Encode(p)
	payload := make([]byte, buff.Len())
	buff.Read(payload)
	ns.pubLock.Lock()
	_, err := ns.pub.SendBytes(payload, 0)
	ns.pubLock.Unlock()

	if err != nil {
		ns.logger.Error.Printf("error broadcasting %s\n", err)
	}
	return err
}

func (ns *ZMQNetworkService) Incoming() <-chan *p2p.Packet {
	return ns.incQueue
}
