package babelqueue

import (
	"crypto/rand"
	"fmt"
)

// uuidV4 returns a random RFC 4122 version-4 UUID string. It uses crypto/rand so
// the core keeps zero external dependencies.
func uuidV4() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand.Read does not fail on supported platforms; if it ever does,
		// the process has no usable entropy and cannot mint safe identifiers.
		panic("babelqueue: unable to read random bytes for UUID: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
