// Package ids generates and validates identifiers.
package ids

import (
	"crypto/rand"
	"fmt"
	"regexp"
)

var uuidRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// NewUUID returns a random RFC 4122 version 4 UUID in canonical lowercase form.
func NewUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ValidUUID reports whether s is a canonical lowercase UUID. Job IDs end up in
// filesystem paths and container names, so they are validated strictly.
func ValidUUID(s string) bool { return uuidRe.MatchString(s) }
