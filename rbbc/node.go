package rbbc

import (
	"imdea.org/redbelly/bc"
	"imdea.org/redbelly/types"
	"imdea.org/redbelly/vrb"

	vrbtypes "imdea.org/redbelly/vrb/types"
)

type Node struct {
	VRB  *vrb.VerifiedReliableBroadcast
	RBBC *RBBCConsensus

	bc *bc.BinaryConsensus
}

func NewNode(
	vrbNS vrbtypes.VRBNetworkService,
	bcNS bc.BCNetworkService,
	n uint,
	t uint,
	pi *types.ProposerIdentity,
	pf ProposalFactory,
) *Node {
	node := &Node{
		VRB: vrb.NewVerifiedReliableBroadcast(vrbNS, pi.Index, n, t),

		bc: bc.NewBinaryConsensus(bcNS, pi.Index, n, t),
	}
	node.RBBC = NewRBBCConsensus(node.VRB, node.bc, n, t, pf)
	return node
}

func (n *Node) Start() {
	n.VRB.Start()
	n.bc.Start()
	n.RBBC.Start()
}

func (n *Node) Stop() {
	n.VRB.Stop()
	n.bc.Stop()
	//TODO n.RBBC.Stop()
}
