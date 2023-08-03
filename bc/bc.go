package bc

import (
	"math"
	"sync"
	"time"

	"imdea.org/redbelly/logging"
	"imdea.org/redbelly/types"
)

const ChannelBuffer = 10000
const NotDecided = -2
const Undefined = -1
const MaxRound = Round(^uint(0) >> 1)

type BinaryConsensus struct {
	DecideChan chan struct {
		Value Value
		PI    types.ProposalIdentifier
	}

	ns       BCNetworkService
	ownIndex types.ProposerIndex
	logger   *logging.Logger

	n uint
	t uint

	//TODO: clean maps
	cvals sync.Map
	bvals sync.Map
	c     sync.Map
	round sync.Map

	bvals_locker sync.RWMutex // protects bvals
	cvals_locker sync.RWMutex // protects cvals

	brEstChan chan *MsgIdentifier
	estChan   chan *Message
	coordChan chan *Message
	auxChan   chan *Message
	stopChan  chan struct{}
}

// NewBinaryConsensus creates an instance of BinaryConsensus where
// ns is the underlying network service
// pi is the proposer index previously agreed upon by nodes
// n is the total number of nodes and
// t is the Byzantine node tolerance
func NewBinaryConsensus(ns BCNetworkService, pi types.ProposerIndex, n uint, t uint) *BinaryConsensus {
	return &BinaryConsensus{
		ns:       ns,
		ownIndex: pi,
		logger:   logging.NewLogger("BC"),

		n: n,
		t: t,

		cvals:        sync.Map{},
		bvals:        sync.Map{},
		c:            sync.Map{},
		round:        sync.Map{},
		bvals_locker: sync.RWMutex{},
		cvals_locker: sync.RWMutex{},
		DecideChan: make(chan struct {
			Value Value
			PI    types.ProposalIdentifier
		}, ChannelBuffer),
		brEstChan: make(chan *MsgIdentifier, ChannelBuffer),
		estChan:   make(chan *Message, ChannelBuffer),
		coordChan: make(chan *Message, ChannelBuffer),
		auxChan:   make(chan *Message, ChannelBuffer),
		stopChan:  make(chan struct{}, 1),
	}
}

func (bc *BinaryConsensus) Start() {
	go bc.incomingLoop()
	go bc.broadcastEstLoop()
	go bc.estLoop()
	go bc.coordLoop()
	go bc.auxLoop()
}

func (bc *BinaryConsensus) Stop() {
	bc.stopChan <- struct{}{}
	close(bc.brEstChan)
	close(bc.estChan)
	close(bc.coordChan)
	close(bc.auxChan)
}

func (bc *BinaryConsensus) waitForCvals(pi types.ProposalIdentifier, r Round) {
	k := struct {
		types.ProposalIdentifier
		Round
	}{pi, r}

	for _, ok := bc.cvals.Load(k); !ok; _, ok = bc.cvals.Load(k) {
	} //TODO: wait?
}

func (bc *BinaryConsensus) getCvals(pi types.ProposalIdentifier, r Round) ValueSet {
	k := struct {
		types.ProposalIdentifier
		Round
	}{pi, r}
	cvals, ok := bc.cvals.Load(k)
	if !ok {
		bc.logger.Warning.Printf("cvals is not defined. waitForCvals should be called before this function")
	}
	return cvals.(ValueSet)
}

func (bc *BinaryConsensus) getW(pi types.ProposalIdentifier, r Round) Value {
	bc.waitForCvals(pi, r)
	cvals := bc.getCvals(pi, r)
	w, ok := cvals.getValue()
	if !ok {
		bc.logger.Warning.Printf("getW trying to get an element from an empty set")
	}
	return w
}

func (bc *BinaryConsensus) getC(pi types.ProposalIdentifier, r Round) Value {
	k := struct {
		types.ProposalIdentifier
		Round
	}{pi, r}
	c, ok := bc.c.Load(k)
	if !ok {
		return Undefined
	}
	return c.(Value)
}

func (bc *BinaryConsensus) getS(pi types.ProposalIdentifier, r Round) ValueSet {
	//TODO: we can compute s inside auxLoop, have s as sync.map and delete bvals and bvals_locker
	k := struct {
		types.ProposalIdentifier
		Round
	}{pi, r}
	for bvalsInt, ok := bc.bvals.Load(k); ; bvalsInt, ok = bc.bvals.Load(k) {
		if !ok {
			continue
		}
		bc.bvals_locker.RLock()
		s := make(ValueSet)
		proposers := make(ProposerIndexSet)
		bvals := bvalsInt.(map[Value]ProposerIndexSet)
		for v, v_proposers := range bvals {
			bc.cvals_locker.RLock()
			v_in_cvals := bc.getCvals(pi, r).contains(v)
			bc.cvals_locker.RUnlock()
			if v_in_cvals {
				if len(v_proposers) >= int(bc.n-bc.t) {
					bc.bvals_locker.RUnlock()
					return singleton(v)
				}
				s.insert(v)
				proposers.Union(v_proposers)
			}
			if len(proposers) >= int(bc.n-bc.t) {
				bc.bvals_locker.RUnlock()
				return s
			}
		}
		bc.bvals_locker.RUnlock()
	}
	bc.logger.Warning.Printf("getS returns an empty set!")
	return make(ValueSet)
}

func (bc *BinaryConsensus) getCurrentRound(pi types.ProposalIdentifier) Round {
	rInt, loaded := bc.round.Load(pi)
	if !loaded {
		return Round(-1)
	}
	return rInt.(Round)
}

func (bc *BinaryConsensus) cleanMaps(pi types.ProposalIdentifier, r Round) {
	k := struct {
		types.ProposalIdentifier
		Round
	}{pi, r}
	bc.cvals.Delete(k)
	bc.bvals.Delete(k)
	bc.c.Delete(k)
}

func (bc *BinaryConsensus) incrementRound(pi types.ProposalIdentifier) {
	r := bc.getCurrentRound(pi)
	if r < Round(1) {
		bc.logger.Error.Printf("Incrementing round for not initialized propose %v", pi)
	}
	bc.round.Store(pi, Round(r+1))
	go bc.cleanMaps(pi, r)
}

func (bc *BinaryConsensus) closePropose(pi types.ProposalIdentifier) {
	r := bc.getCurrentRound(pi)
	if r < Round(1) {
		bc.logger.Error.Printf("Incrementing round for not initialized propose %v", pi)
	}
	bc.round.Store(pi, MaxRound)
	go bc.cleanMaps(pi, r)
}

func (bc *BinaryConsensus) Propose(val Value, pi types.ProposalIdentifier) {
	go func(val Value, pi types.ProposalIdentifier) {
		_, loaded := bc.round.LoadOrStore(pi, Round(1))
		if loaded {
			bc.logger.Warning.Printf("Trying to propose more than once for propose %v", pi)
			return
		}
		decided := Round(NotDecided)
		for {
			r := bc.getCurrentRound(pi)
			bc.logger.Verbose.Printf("Broadcasting est %v at round %v for %v", val, r, pi)
			bc.brEstChan <- NewMsgIdentifier(pi, r, val)
			timer := time.NewTimer(time.Duration(math.Log10(float64(r*Round(4)))) * time.Second)
			if uint(r)%bc.n == uint(bc.ownIndex) {
				w := bc.getW(pi, r)
				bc.ns.BCBroadcast(NewCoordMessage(pi, r, w))
			}
			<-timer.C
			bc.waitForCvals(pi, r)
			c := bc.getC(pi, r) //can be undefined
			var e ValueSet
			bc.cvals_locker.RLock()
			c_in_cvals := bc.getCvals(pi, r).contains(c)
			bc.cvals_locker.RUnlock()
			if c_in_cvals {
				e = singleton(c)
			} else {
				bc.cvals_locker.RLock()
				cvals := bc.getCvals(pi, r)
				e = ValueSet{}
				for x := range cvals {
					e.insert(x)
				}
				bc.cvals_locker.RUnlock()
			}
			bc.ns.BCBroadcast(NewAuxMessage(pi, r, e, bc.ownIndex))
			s := bc.getS(pi, r)
			if len(s) == 1 {
				v, ok := s.getValue()
				if !ok {
					bc.logger.Warning.Printf("Propose: trying to get an element from an empty set")
				}
				val = v
				if int(r)%2 == int(v) && decided == NotDecided {
					decided = r
					bc.logger.Info.Printf("Agreed %d on proposal %d of round %d\n", val, pi.ProposerIndex, pi.Round)
					bc.DecideChan <- struct {
						Value Value
						PI    types.ProposalIdentifier
					}{
						Value: val,
						PI:    pi,
					}
				}
			} else {
				val = Value(r % 2)
			}
			if decided == r-2 {
				go bc.closePropose(pi)
				return
			}
			bc.incrementRound(pi)
		}
	}(val, pi)
}

func (bc *BinaryConsensus) incomingLoop() {
main:
	for {
		select {
		case <-bc.stopChan:
			break main
		case msg := <-bc.ns.BCIncoming():
			switch msg.Type {
			case Coord:
				bc.coordChan <- msg
			case Aux:
				bc.auxChan <- msg
			case Est:
				bc.estChan <- msg
			}
		}
	}
}

func (bc *BinaryConsensus) broadcastEstLoop() {
	broadcasted := make(map[MsgIdentifier]bool)
	for msg := range bc.brEstChan {
		bc.logger.Debug.Printf("V:%d R:%d ID:%d|broadcasting", msg.Value, msg.Round, msg.ProposalIdentifier)
		if broadcasted[*msg] {
			bc.logger.Debug.Printf("V:%d R:%d ID:%d| Already broadcasted", msg.Value, msg.Round, msg.ProposalIdentifier)
			continue
		}
		bc.ns.BCBroadcast(NewEstMessage(msg.ProposalIdentifier, msg.Round, msg.Value, bc.ownIndex))
		broadcasted[*msg] = true
	}

}

func (bc *BinaryConsensus) estLoop() {
	witness := make(map[MsgIdentifier]ProposerIndexSet)
	processed := make(map[MsgIdentifier]bool)
	for msg := range bc.estChan {
		bc.logger.Debug.Printf("V:%d R:%d ID:%d|EST received", msg.Value, msg.Round, msg.ProposalIdentifier)
		mi := NewMsgIdentifier(msg.ProposalIdentifier, msg.Round, msg.Value)
		if processed[*mi] {
			continue
		}
		if witness[*mi] == nil {
			witness[*mi] = make(ProposerIndexSet)
		}
		witness[*mi].Add(msg.Sender)
		if len(witness[*mi]) == int(bc.t)+1 {
			bc.brEstChan <- mi
		}
		if len(witness[*mi]) == int(2*bc.t)+1 {
			processed[*mi] = true
			delete(witness, *mi)
			if msg.Round < bc.getCurrentRound(msg.ProposalIdentifier) {
				bc.logger.Debug.Printf("V:%d R:%d ID:%d|EST discarded", msg.Value, msg.Round, msg.ProposalIdentifier)
				continue
			}
			go func(msg *Message) {
				bc.logger.Debug.Printf("V:%d R:%d ID:%d|EST adding to cvals", msg.Value, msg.Round, msg.ProposalIdentifier)
				k := struct {
					types.ProposalIdentifier
					Round
				}{msg.ProposalIdentifier, msg.Round}
				bc.cvals_locker.Lock()
				cvalsInt, loaded := bc.cvals.LoadOrStore(k, singleton(msg.Value))
				if loaded {
					cvals := cvalsInt.(ValueSet)
					if !cvals.contains(msg.Value) {
						cvals.insert(msg.Value)
						bc.cvals.Store(k, cvals)
					}
				}
				bc.cvals_locker.Unlock()
			}(msg)
		}
	}
}

func (bc *BinaryConsensus) coordLoop() {
	for msg := range bc.coordChan {
		if msg.Round < bc.getCurrentRound(msg.ProposalIdentifier) {
			bc.logger.Debug.Printf("V:%d R:%d ID:%d|COORD discarded", msg.Value, msg.Round, msg.ProposalIdentifier)
			continue
		}
		bc.logger.Debug.Printf("V:%d R:%d ID:%d|COORD received", msg.Value, msg.Round, msg.ProposalIdentifier)
		k := struct {
			types.ProposalIdentifier
			Round
		}{msg.ProposalIdentifier, msg.Round}
		_, loaded := bc.c.LoadOrStore(k, msg.Value)
		if loaded {
			bc.logger.Warning.Printf("V:%d R:%d I:%d|COORD received multiple times", msg.Value, msg.Round, msg.ProposalIdentifier)
		}
	}
}

func (bc *BinaryConsensus) auxLoop() {
	for msg := range bc.auxChan {
		if msg.Round < bc.getCurrentRound(msg.ProposalIdentifier) {
			bc.logger.Debug.Printf("V:%d R:%d ID:%d|AUX discarded", msg.Value, msg.Round, msg.ProposalIdentifier)
			continue
		}
		go func(msg *Message) {
			bc.logger.Debug.Printf("SV:%d R:%d ID:%d|AUX received from %d", msg.ValueSet, msg.Round, msg.ProposalIdentifier, msg.Sender)

			k := struct {
				types.ProposalIdentifier
				Round
			}{msg.ProposalIdentifier, msg.Round}
			bc.bvals_locker.Lock()
			bvalsInt, loaded := bc.bvals.Load(k)
			var bvals map[Value]ProposerIndexSet
			if !loaded {
				bvals = map[Value]ProposerIndexSet{
					0: make(ProposerIndexSet),
					1: make(ProposerIndexSet),
				}
			} else {
				bvals = bvalsInt.(map[Value]ProposerIndexSet)
			}
			for v := range msg.ValueSet {
				bvals[v].Add(msg.Sender)
			}
			bc.bvals.Store(k, bvals)
			bc.bvals_locker.Unlock()
		}(msg)
	}
}
