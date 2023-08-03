package rbbc

import (
	"time"

	"imdea.org/redbelly/types"
)

var DefaultProposalTimeout = 5 * time.Second

type ProposalSigner struct {
	Identity *types.ProposerIdentity
	Mempool  types.Mempool
	timeout  time.Duration
}

func NewProposalSigner(pi *types.ProposerIdentity, m types.Mempool) *ProposalSigner {
	return &ProposalSigner{
		Identity: pi,
		Mempool:  m,
		timeout:  DefaultProposalTimeout,
	}
}

func (ps *ProposalSigner) GetProposal(r types.RBBCRound) (*types.Proposal, *time.Time) {
	timeout := time.Now().Add(ps.timeout)
	return types.NewProposal(
		ps.Mempool.Get(),
		ps.Identity,
		r,
	), &timeout
}
