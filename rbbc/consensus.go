package rbbc

import (
	"sync"
	"time"

	"imdea.org/redbelly/bc"
	"imdea.org/redbelly/logging"
	"imdea.org/redbelly/types"
	"imdea.org/redbelly/vrb"
)

const ChannelBuffer = 10000

type RBBCConsensus struct {
	DecideChan       chan *types.Block
	ReconciliateFunc ReconciliateFunc

	vrb *vrb.VerifiedReliableBroadcast
	bc  *bc.BinaryConsensus

	n               uint
	t               uint
	proposalFactory ProposalFactory

	round          types.RBBCRound
	roundLock      *sync.RWMutex
	agreeCount     int
	agreeCountLock *sync.Mutex
	bitmask        map[types.ProposerIndex]bool
	proposals      map[types.ProposerIndex]*types.Proposal
	proposalsCond  *sync.Cond
	roundTimeout   *time.Timer
	roundOpen      bool
	roundOpenCond  *sync.Cond
	roundEndTime   *time.Time //TODO: use it for waiting time between two rounds

	idle     bool
	idleLock *sync.RWMutex

	proposalsQueue  chan *types.Proposal
	bcDecisionQueue chan struct {
		Value bc.Value
		PI    types.ProposalIdentifier
	}
	logger *logging.Logger
}

func NewRBBCConsensus(vrb *vrb.VerifiedReliableBroadcast, bincons *bc.BinaryConsensus, n uint, t uint, pf ProposalFactory) *RBBCConsensus {
	return &RBBCConsensus{
		DecideChan:      make(chan *types.Block, ChannelBuffer),
		vrb:             vrb,
		bc:              bincons,
		n:               n,
		t:               t,
		proposalFactory: pf,
		agreeCountLock:  &sync.Mutex{},
		bitmask:         make(map[types.ProposerIndex]bool),
		proposals:       make(map[types.ProposerIndex]*types.Proposal),
		proposalsCond:   sync.NewCond(&sync.Mutex{}),
		round:           0,
		roundLock:       &sync.RWMutex{},
		roundOpenCond:   sync.NewCond(&sync.Mutex{}),

		idle:           true,
		idleLock:       &sync.RWMutex{},
		proposalsQueue: make(chan *types.Proposal, ChannelBuffer),
		bcDecisionQueue: make(chan struct {
			Value bc.Value
			PI    types.ProposalIdentifier
		}, ChannelBuffer),
		logger: logging.NewLogger("RBBC"),
	}
}

func (rbbc *RBBCConsensus) Start() {
	go rbbc.proposalsManager()
	go rbbc.incomingBinDecisionsManager()
	go rbbc.getBinDecisions()
}

func (rbbc *RBBCConsensus) Initiate() bool {
	return rbbc.newRound(true)
}

func (rbbc *RBBCConsensus) proposalsManager() {
	for p := range rbbc.vrb.DeliveryChan {
		if p.Proposal.Round > rbbc.GetCurrentRound() {
			rbbc.newRound(true)
		}
		currentRound := rbbc.GetCurrentRound()
		p.Proposal.Txs = types.PurgeTxList(p.Proposal.Txs, p.InvalidIndexes)
		rbbc.manageProposal(p.Proposal, currentRound)
	}
}

func (rbbc *RBBCConsensus) manageEnqueuedProposals() {
	var p *types.Proposal
	pBuff := make([]*types.Proposal, 0, len(rbbc.proposalsQueue))
rcvLoop:
	for {
		select {
		case p = <-rbbc.proposalsQueue:
			pBuff = append(pBuff, p) //can't use direct add to the array because a proposal can arrive in the meanwhile
		default:
			break rcvLoop
		}
	}
	for _, p := range pBuff {
		round := rbbc.GetCurrentRound()
		rbbc.logger.Debug.Printf("Managing enqueued proposal from %d for round %d\n", p.Proposer.Index, p.Round)
		rbbc.manageProposal(p, round)
	}
}

func (rbbc *RBBCConsensus) manageProposalRound(p *types.Proposal, r types.RBBCRound) bool {
	switch {
	case p.Round == r:
		return true
	case p.Round < r:
		rbbc.logger.Debug.Printf("Proposal for previous round %d from proposer %d discarded. Current %d\n", p.Round, p.Proposer.Index, r)
		return false
	default:
		rbbc.proposalsQueue <- p
		rbbc.logger.Info.Printf("Future proposal for round %d (%d) from proposer %d enqueued. Queue %d\n", p.Round, r, p.Proposer.Index, len(rbbc.proposalsQueue))
		return false
	}
}

func (rbbc *RBBCConsensus) manageProposal(p *types.Proposal, r types.RBBCRound) {
	if !rbbc.manageProposalRound(p, r) {
		return
	}
	rbbc.proposalsCond.L.Lock()
	if _, found := rbbc.proposals[p.Proposer.Index]; found {
		rbbc.proposalsCond.L.Unlock()
		//TODO: save double proposal. Maybe the winning one is the second and not the first
		rbbc.logger.Warning.Printf("Discarded double proposal from proposer %d\n", p.Proposer.Index)
		return
	}
	rbbc.proposals[p.Proposer.Index] = p
	rbbc.proposalsCond.L.Unlock()
	rbbc.proposalsCond.Broadcast()
	if rbbc.IsRoundOpen() {
		rbbc.bc.Propose(bc.NewValue(true), *p.GetIdentifier())
	}
}

// TODO stop condition for main loop
func (rbbc *RBBCConsensus) incomingBinDecisionsManager() {
	// Waits the first round to be opened
	rbbc.roundOpenCond.L.Lock()
	for !rbbc.roundOpen {
		rbbc.roundOpenCond.Wait()
	}
	rbbc.roundOpenCond.L.Unlock()
	for {
	proposalsDecisionLoop:
		for {
			select {
			case decision := <-rbbc.bcDecisionQueue:
				currentRound := rbbc.GetCurrentRound() //can't put before the select otherwise could use an old round number
				if decision.PI.Round < currentRound {
					rbbc.logger.Warning.Printf("Discarded decision for proposal %d in round %d. Current %d\n", decision.PI.ProposerIndex, decision.PI.Round, currentRound)
					continue
				}
				if decision.PI.Round > currentRound {
					rbbc.bcDecisionQueue <- decision
					rbbc.logger.Debug.Printf("Reinserting decision for proposal %d in round %d. Current %d\n", decision.PI.ProposerIndex, decision.PI.Round, currentRound)
					continue
				}
				rbbc.logger.Verbose.Printf("Managing decision %t for proposal %d at round %d (Queue: %d)\n", decision.Value.GetBool(), decision.PI.ProposerIndex, currentRound, len(rbbc.bc.DecideChan))
				rbbc.bitmask[decision.PI.ProposerIndex] = decision.Value.GetBool()
				if decision.Value.GetBool() {
					if rbbc.incrementAndGetAgreeCount() == int(rbbc.n-rbbc.t) && rbbc.IsRoundOpen() {
						if !rbbc.roundTimeout.Stop() {
							<-rbbc.roundTimeout.C
						}
						rbbc.logger.Debug.Printf("Cleaning timeout timer for round %d\n", currentRound)
						rbbc.closeRound()
					}
				}
				if len(rbbc.bitmask) == int(rbbc.n) {
					rbbc.logger.Debug.Printf("Decision bitmap at round %d completed\n", currentRound)
					break proposalsDecisionLoop
				}
			case <-rbbc.roundTimeout.C:
				currentRound := rbbc.GetCurrentRound()
				rbbc.logger.Warning.Printf("Proposal timeout expired for round %d\n", currentRound)
				rbbc.closeRound()
			}
		}
		rbbc.reconciliate()
		if len(rbbc.proposalsQueue) > 0 {
			rbbc.newRound(false)
		} else {
			rbbc.idleLock.Lock()
			rbbc.idle = true
			rbbc.logger.Info.Println("Idle")
			rbbc.idleLock.Unlock()
		}
	}
}

func (rbbc *RBBCConsensus) reconciliate() {
	currentRound := rbbc.GetCurrentRound()
	rbbc.logger.Debug.Printf("Reconciliating for round %d\n", currentRound)
	proposals := make([]*types.Proposal, 0)
	for i := uint(0); i < rbbc.n; i++ {
		si := types.ProposerIndex((i + uint(rbbc.round)) % rbbc.n)
		if rbbc.bitmask[si] {
			rbbc.proposalsCond.L.Lock()
			for rbbc.proposals[si] == nil {
				rbbc.logger.Debug.Printf("Waiting proposal %d to arrive in round %d\n", si, currentRound)
				rbbc.proposalsCond.Wait()
			}
			proposals = append(proposals, rbbc.proposals[si])
			rbbc.proposalsCond.L.Unlock()
		}
	}
	block := rbbc.ReconciliateFunc(proposals, currentRound)
	select {
	case rbbc.DecideChan <- block:
	default:
		rbbc.logger.Warning.Printf("Delivery buffer is full for block %d\n", block.R)
	}
}

// newRound is the utility function meant to initialize data structure for a new round.
// For this reason it needs exclusive access to all the round related variables.
func (rbbc *RBBCConsensus) newRound(checkIsIdle bool) bool {
	rbbc.roundLock.Lock()
	if checkIsIdle && !rbbc.IsIdle() {
		rbbc.roundLock.Unlock()
		return false
	}
	rbbc.idleLock.Lock()
	rbbc.proposalsCond.L.Lock()
	rbbc.roundOpenCond.L.Lock()
	rbbc.logger.Debug.Printf("Proceeding to round %d\n", rbbc.round+1)
	rbbc.proposals = make(map[types.ProposerIndex]*types.Proposal)
	rbbc.bitmask = make(map[types.ProposerIndex]bool)
	rbbc.initAgreeCount()
	rbbc.round++
	rbbc.idle = false
	p, t := rbbc.proposalFactory.GetProposal(rbbc.round) //TODO: wrap in a goroutine
	rbbc.logger.Info.Printf("Proposing %s at round %d\n", p.Hash.Fingerprint(), rbbc.round)
	rbbc.vrb.Broadcast(p)
	rbbc.roundTimeout = time.NewTimer(t.Sub(time.Now()))
	rbbc.roundOpen = true
	rbbc.roundOpenCond.L.Unlock()
	rbbc.roundOpenCond.Broadcast()
	rbbc.proposalsCond.L.Unlock()
	rbbc.idleLock.Unlock()
	rbbc.roundLock.Unlock()
	go rbbc.manageEnqueuedProposals()
	return true
}

func (rbbc *RBBCConsensus) closeRound() {
	currentRound := rbbc.GetCurrentRound()
	rbbc.roundOpenCond.L.Lock()
	rbbc.roundOpen = false
	rbbc.roundOpenCond.L.Unlock()
	rbbc.proposalsCond.L.Lock()
	for i := 0; i < int(rbbc.n); i++ {
		if _, arrived := rbbc.proposals[types.ProposerIndex(i)]; !arrived {
			rbbc.bc.Propose(bc.NewValue(false), *types.NewProposalIdentifier(types.ProposerIndex(i), currentRound))
		}
	}
	rbbc.proposalsCond.L.Unlock()
	rbbc.logger.Debug.Printf("Round %d closed\n", currentRound)
}

func (rbbc *RBBCConsensus) GetCurrentRound() types.RBBCRound {
	rbbc.roundLock.RLock()
	defer rbbc.roundLock.RUnlock()
	return rbbc.round
}

func (rbbc *RBBCConsensus) IsRoundOpen() bool {
	rbbc.roundOpenCond.L.Lock()
	defer rbbc.roundOpenCond.L.Unlock()
	return rbbc.roundOpen
}

func (rbbc *RBBCConsensus) IsIdle() bool {
	rbbc.idleLock.RLock()
	defer rbbc.idleLock.RUnlock()
	return rbbc.idle
}

func (rbbc *RBBCConsensus) incrementAndGetAgreeCount() int {
	rbbc.agreeCountLock.Lock()
	defer rbbc.agreeCountLock.Unlock()
	rbbc.agreeCount++
	return rbbc.agreeCount
}

func (rbbc *RBBCConsensus) initAgreeCount() {
	rbbc.agreeCountLock.Lock()
	defer rbbc.agreeCountLock.Unlock()
	rbbc.agreeCount = 0
}

func (rbbc *RBBCConsensus) getBinDecisions() {
	for {
		decision := <-rbbc.bc.DecideChan
		rbbc.bcDecisionQueue <- decision
	}
}
