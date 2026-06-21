package gdpr_test

import (
	"errors"
	"testing"

	"github.com/babelqueue/babelqueue-go/gdpr"
)

func TestAESGCMCipher_RoundTrip(t *testing.T) {
	c := cipherFor(t, newKey(t))
	cases := [][]byte{
		[]byte(`"alice@example.com"`),          // a JSON string leaf
		[]byte(`1042`),                         // a JSON number leaf
		[]byte(`{"pan":"4111","exp":"12/30"}`), // a JSON object leaf
		[]byte(`null`),
		{}, // empty plaintext
	}
	for _, pt := range cases {
		ct, err := c.Encrypt(pt)
		if err != nil {
			t.Fatalf("Encrypt(%q): %v", pt, err)
		}
		if ct == string(pt) {
			t.Errorf("ciphertext equals plaintext for %q", pt)
		}
		got, err := c.Decrypt(ct)
		if err != nil {
			t.Fatalf("Decrypt: %v", err)
		}
		if string(got) != string(pt) {
			t.Errorf("round-trip mismatch: got %q want %q", got, pt)
		}
	}
}

func TestAESGCMCipher_NonceIsRandom(t *testing.T) {
	c := cipherFor(t, newKey(t))
	pt := []byte(`"same value"`)
	a, _ := c.Encrypt(pt)
	b, _ := c.Encrypt(pt)
	if a == b {
		t.Fatal("two Encrypt calls on the same plaintext produced identical ciphertext (nonce not random)")
	}
	// Both still decrypt to the same plaintext.
	for _, ct := range []string{a, b} {
		got, err := c.Decrypt(ct)
		if err != nil || string(got) != string(pt) {
			t.Fatalf("Decrypt(%q) = %q, %v", ct, got, err)
		}
	}
}

func TestNewAESGCMCipher_KeySizes(t *testing.T) {
	for _, n := range []int{16, 24, 32} {
		if _, err := gdpr.NewAESGCMCipher(make([]byte, n)); err != nil {
			t.Errorf("%d-byte key should be accepted: %v", n, err)
		}
	}
	for _, n := range []int{0, 8, 15, 31, 33, 64} {
		_, err := gdpr.NewAESGCMCipher(make([]byte, n))
		if !errors.Is(err, gdpr.ErrInvalidKeySize) {
			t.Errorf("%d-byte key should be rejected with ErrInvalidKeySize, got %v", n, err)
		}
	}
}

func TestAESGCMCipher_DecryptMalformed(t *testing.T) {
	c := cipherFor(t, newKey(t))

	if _, err := c.Decrypt("!!!not base64!!!"); !errors.Is(err, gdpr.ErrMalformedCiphertext) {
		t.Errorf("non-base64 input should be ErrMalformedCiphertext, got %v", err)
	}
	// Valid base64 but too short to hold a 12-byte GCM nonce.
	if _, err := c.Decrypt("AAAA"); !errors.Is(err, gdpr.ErrMalformedCiphertext) {
		t.Errorf("too-short input should be ErrMalformedCiphertext, got %v", err)
	}
}

func TestAESGCMCipher_DecryptWrongKey(t *testing.T) {
	a := cipherFor(t, newKey(t))
	b := cipherFor(t, newKey(t))
	ct, err := a.Encrypt([]byte(`"secret"`))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if _, err := b.Decrypt(ct); err == nil {
		t.Fatal("decrypting under a different key must fail (GCM authentication)")
	}
}
