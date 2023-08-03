package dpo

import "imdea.org/redbelly/types"

type IDPO interface {
	Add(types.Transaction) (bool, error)
	Get() (*Storage, error)
	EpochInc(types.Epoch) error
}
