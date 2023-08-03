package rbbc

import (
	"time"

	"imdea.org/redbelly/types"
)

type ProposalFactory interface {
	GetProposal(types.RBBCRound) (*types.Proposal, *time.Time)
}

type ReconciliateFunc func([]*types.Proposal, types.RBBCRound) *types.Block
