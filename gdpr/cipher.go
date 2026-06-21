package gdpr

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
)

// Cipher is the field-level protection primitive that the CALLER provides — a seam onto a KMS,
// Vault transit, an HSM, a tokenisation service, or the reference AESGCMCipher below. Protect
// runs Encrypt over every x-gdpr-sensitive leaf's value (after it is canonically JSON-encoded);
// Unprotect runs Decrypt to restore it. Keeping this an interface is what holds GR-7: the core
// module never pulls a crypto/KMS dependency — only a caller who imports a concrete backend does.
//
// Contract for an implementation:
//
//   - Encrypt takes the canonical JSON bytes of one field value (see Protect) and returns the
//     ciphertext as a STRING — it MUST be valid for placement inside a JSON document (the
//     AESGCMCipher reference returns base64, which is). The same plaintext MAY encrypt to
//     different strings each call (a random nonce/IV is expected and good).
//   - Decrypt is the exact inverse: given a string Encrypt produced, it returns the original
//     JSON bytes byte-for-byte. A string it did not produce, or one produced under a different
//     key, MUST return an error rather than silent garbage, so a wrong-key consume fails loudly.
//   - Both MUST be safe for concurrent use; a producer/consumer fans the same Cipher across goroutines.
type Cipher interface {
	// Encrypt protects one field value (its canonical JSON bytes) and returns a JSON-safe
	// ciphertext string.
	Encrypt(plaintext []byte) (string, error)
	// Decrypt reverses Encrypt, returning the original field-value JSON bytes.
	Decrypt(ciphertext string) ([]byte, error)
}

// AESGCMCipher is a reference [Cipher] built ONLY on the Go standard library
// (crypto/aes + crypto/cipher + crypto/rand): AES-GCM authenticated encryption with a fresh
// random nonce per call, the nonce prepended to the ciphertext, the whole thing base64
// (std, padded) encoded so it drops straight into a JSON string. The key is the CALLER's — this
// type performs no key management, rotation or derivation; bind a KMS-backed Cipher for that.
//
// AES-256-GCM is used when the key is 32 bytes (the recommended size); 16- and 24-byte keys
// select AES-128/192-GCM. GCM authenticates the ciphertext, so Decrypt rejects any tampered or
// wrong-key input with an error (it does not return corrupt plaintext). It is zero-dependency and
// safe for concurrent use (the cipher.AEAD is created once and only read).
type AESGCMCipher struct {
	aead cipher.AEAD
}

// ErrInvalidKeySize is returned by NewAESGCMCipher when the key is not 16, 24, or 32 bytes
// (AES-128/192/256). Detect it with errors.Is.
var ErrInvalidKeySize = errors.New("babelqueue/gdpr: AES key must be 16, 24, or 32 bytes")

// ErrMalformedCiphertext is returned by AESGCMCipher.Decrypt when the input is not valid
// base64 or is too short to contain a nonce — i.e. not something this cipher produced.
// Detect it with errors.Is.
var ErrMalformedCiphertext = errors.New("babelqueue/gdpr: malformed ciphertext")

// NewAESGCMCipher builds an AES-GCM reference cipher from a raw symmetric key. The key length
// selects the AES variant: 32 bytes → AES-256-GCM (recommended), 24 → AES-192, 16 → AES-128.
// Any other length returns ErrInvalidKeySize.
func NewAESGCMCipher(key []byte) (*AESGCMCipher, error) {
	switch len(key) {
	case 16, 24, 32:
	default:
		return nil, fmt.Errorf("%w (got %d)", ErrInvalidKeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("babelqueue/gdpr: new cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("babelqueue/gdpr: new GCM: %w", err)
	}
	return &AESGCMCipher{aead: aead}, nil
}

// Encrypt seals plaintext with a fresh random nonce, prepends the nonce, and base64-encodes the
// result. Implements [Cipher].
func (c *AESGCMCipher) Encrypt(plaintext []byte) (string, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("babelqueue/gdpr: nonce: %w", err)
	}
	// Seal appends the ciphertext+tag to its first argument; passing nonce there yields
	// nonce||ciphertext||tag in one slice, which Decrypt splits back apart.
	sealed := c.aead.Seal(nonce, nonce, plaintext, nil)
	return base64.StdEncoding.EncodeToString(sealed), nil
}

// Decrypt reverses Encrypt: it base64-decodes, splits off the prepended nonce, and opens the
// GCM ciphertext. A wrong key or tampered input fails GCM authentication and returns an error
// (never corrupt plaintext). Implements [Cipher].
func (c *AESGCMCipher) Decrypt(ciphertext string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return nil, fmt.Errorf("%w: not base64: %v", ErrMalformedCiphertext, err)
	}
	ns := c.aead.NonceSize()
	if len(raw) < ns {
		return nil, fmt.Errorf("%w: shorter than nonce", ErrMalformedCiphertext)
	}
	nonce, sealed := raw[:ns], raw[ns:]
	plaintext, err := c.aead.Open(nil, nonce, sealed, nil)
	if err != nil {
		// GCM authentication failed: wrong key, tampered ciphertext, or not our output.
		return nil, fmt.Errorf("babelqueue/gdpr: decrypt: %w", err)
	}
	return plaintext, nil
}
