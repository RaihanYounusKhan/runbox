// Package auth generates and hashes API keys.
//
// Keys look like "rbx_<43 base64url chars>" (256 bits of entropy). Only the
// SHA-256 of a key is stored. A slow password hash (bcrypt/argon2) isn't
// needed because the keys are random and high-entropy, not human-chosen, and
// a fast hash keeps per-request authentication cheap.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"strings"
)

// Prefix identifies runbox keys (and makes them easy to find with secret scanners).
const Prefix = "rbx_"

// NewKey returns a fresh random API key.
func NewKey() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return Prefix + base64.RawURLEncoding.EncodeToString(b)
}

// Hash returns the stored form of a key.
func Hash(key string) []byte {
	h := sha256.Sum256([]byte(key))
	return h[:]
}

// DisplayPrefix returns the first few characters, safe to show in listings.
func DisplayPrefix(key string) string {
	if len(key) < len(Prefix)+6 {
		return key
	}
	return key[:len(Prefix)+6]
}

// BearerToken extracts the token from an "Authorization: Bearer ..." header.
func BearerToken(header string) (string, bool) {
	const p = "Bearer "
	if len(header) <= len(p) || !strings.EqualFold(header[:len(p)], p) {
		return "", false
	}
	return strings.TrimSpace(header[len(p):]), true
}

// ConstantTimeEqual compares secrets without leaking timing information.
func ConstantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
