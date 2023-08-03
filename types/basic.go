package types

import (
	"crypto/sha256"
	"encoding/hex"
)

type Hash [sha256.Size]byte

func (h Hash) Fingerprint() string {
	dst := make([]byte, 6)
	hex.Encode(dst, h[:3])
	return string(dst)
}
