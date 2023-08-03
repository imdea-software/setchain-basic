package types

import (
	"imdea.org/redbelly/types"
)

type MessageType uint8

const (
	Init MessageType = iota
	Echo
	Ready
)

type Message struct {
	Type                   MessageType
	Proposal               *types.Proposal
	ProposalDigest         *types.Hash
	ProposalInvalidIndexes []types.TransactionIndex
	ProposerIndex          types.ProposerIndex
}

func NewInitMessage(p *types.Proposal, pi types.ProposerIndex) *Message {
	return &Message{
		Type:          Init,
		Proposal:      p,
		ProposerIndex: pi,
	}
}

func NewEchoMessage(pd *types.Hash, pi types.ProposerIndex) *Message {
	return &Message{
		Type:           Echo,
		ProposalDigest: pd,
		ProposerIndex:  pi,
	}
}

func NewReadyMessage(pd *types.Hash, pi types.ProposerIndex) *Message {
	return &Message{
		Type:                   Ready,
		ProposalDigest:         pd,
		ProposalInvalidIndexes: make([]types.TransactionIndex, 0),
		ProposerIndex:          pi,
	}
}

type VRBNetworkService interface {
	VRBBroadcast(*Message)
	VRBIncoming() <-chan *Message
}

type VerifierRole uint8

const (
	Primary VerifierRole = iota
	Secondary
	No
)

type VerificationLogic interface {
	// VerifyProposal specifies the validation result in the same order transaction are in the proposal.
	// The returned array includes only the invalid indexes.
	VerifyProposal(p *types.Proposal) []types.TransactionIndex
	// VerifyRole outputs the role of the specified proposer for the given proposal
	VerifyRole(p *types.Proposal, pi types.ProposerIndex) VerifierRole
}
