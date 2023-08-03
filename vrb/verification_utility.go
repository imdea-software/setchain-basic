package vrb

import (
	"imdea.org/redbelly/types"

	vrbtypes "imdea.org/redbelly/vrb/types"
)

// VerificationUtility provide basic verification with a round robin policy on verifier choices
type VerificationUtility struct {
	N types.RBBCRound
	T types.RBBCRound
	V types.VerifyTransaction
}

// VerifyProposal returns the slice of indexes of invalid transactions.
func (vu *VerificationUtility) VerifyProposal(p *types.Proposal) []types.TransactionIndex {
	invalid := make([]types.TransactionIndex, 0)
	for i, tx := range p.Txs {
		if !vu.V(tx) {
			invalid = append(invalid, types.TransactionIndex(i))
		}
	}
	return invalid
}

// VerifyRole uses the proposer index and the round to do a round robin among proposers.
func (vu *VerificationUtility) VerifyRole(p *types.Proposal, pi types.ProposerIndex) vrbtypes.VerifierRole {
	linPropIndex := types.RBBCRound(pi) + (p.Round - p.Round%vu.N)
	if linPropIndex < p.Round {
		linPropIndex += vu.N
	}
	switch {
	case linPropIndex <= p.Round+vu.T && linPropIndex >= p.Round:
		return vrbtypes.Primary
	case linPropIndex <= p.Round+vu.T*2 && linPropIndex > p.Round+vu.T:
		return vrbtypes.Secondary
	default:
		return vrbtypes.No
	}
}
