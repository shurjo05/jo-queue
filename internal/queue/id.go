package queue

import (
	"crypto/rand"
	"encoding/hex"
)

// newID returns a random 128-bit ID as 32 hex characters. With 2^128
// possible values, two IDs colliding is not a practical concern, so no
// coordination between servers is needed to hand out IDs.
func newID() string {
	var b [16]byte
	// crypto/rand.Read never returns an error on supported platforms
	// (Go 1.24+ panics internally instead), so the error is ignored.
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
