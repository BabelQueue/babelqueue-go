package gdpr_test

import (
	"encoding/base64"
	"errors"
	"testing"

	"github.com/babelqueue/babelqueue-go/gdpr"
	"github.com/babelqueue/babelqueue-go/schema"
)

// failCipher is a Cipher whose Encrypt always errors — used to assert Protect surfaces a cipher
// failure wrapped, so a caller can refuse to publish a partially-protected message.
type failCipher struct{ err error }

func (f failCipher) Encrypt([]byte) (string, error) { return "", f.err }
func (f failCipher) Decrypt(string) ([]byte, error) { return nil, f.err }

func TestProtect_CipherErrorSurfaces(t *testing.T) {
	sentinel := errors.New("kms unavailable")
	sch := parseSchema(t, `{"type":"object","properties":{"email":{"type":"string","x-gdpr-sensitive":true}}}`)
	data := map[string]any{"email": "a@b.com"}

	err := gdpr.Protect(data, sch, failCipher{err: sentinel})
	if err == nil || !errors.Is(err, sentinel) {
		t.Fatalf("Protect should surface the cipher error wrapped, got %v", err)
	}
}

func TestProtect_TypeMismatchesSkipped(t *testing.T) {
	c := cipherFor(t, newKey(t))

	// Schema expects profile to be an object and addresses an array of objects, but the message
	// carries scalars where the path descends — every such mismatch is skipped, not an error,
	// and the scalar is left untouched.
	sch := parseSchema(t, `{
		"type":"object",
		"properties":{
			"profile":{"type":"object","properties":{"full_name":{"type":"string","x-gdpr-sensitive":true}}},
			"addresses":{"type":"array","items":{"type":"object","properties":{"line":{"type":"string","x-gdpr-sensitive":true}}}}
		}
	}`)

	data := map[string]any{
		"profile":   "not-an-object", // path wants to descend into an object
		"addresses": "not-an-array",  // path wants to descend into an array
	}
	if err := gdpr.Protect(data, sch, c); err != nil {
		t.Fatalf("Protect with type mismatches: %v", err)
	}
	if data["profile"] != "not-an-object" || data["addresses"] != "not-an-array" {
		t.Fatalf("type-mismatched values must be left untouched, got %v", data)
	}

	// An array whose ELEMENTS are scalars (not objects) where the path expects to descend further.
	sch2 := parseSchema(t, `{"type":"object","properties":{
		"addresses":{"type":"array","items":{"type":"object","properties":{"line":{"type":"string","x-gdpr-sensitive":true}}}}
	}}`)
	data2 := map[string]any{"addresses": []any{"scalar-element"}}
	if err := gdpr.Protect(data2, sch2, c); err != nil {
		t.Fatalf("Protect with scalar array elements: %v", err)
	}
	if data2["addresses"].([]any)[0] != "scalar-element" {
		t.Fatalf("scalar array element must be left untouched, got %v", data2["addresses"])
	}
}

func TestProtect_DirectArrayItemMark(t *testing.T) {
	// An array of scalars whose ITEMS themselves are marked sensitive (items.x-gdpr-sensitive),
	// exercising the array-leaf branch of applyAtPath.
	c := cipherFor(t, newKey(t))
	sch := parseSchema(t, `{"type":"object","properties":{
		"tags":{"type":"array","items":{"type":"string","x-gdpr-sensitive":true}}
	}}`)
	data := map[string]any{"tags": []any{"vip", "fraud-watch"}}

	if err := gdpr.Protect(data, sch, c); err != nil {
		t.Fatalf("Protect: %v", err)
	}
	for i, v := range data["tags"].([]any) {
		if s, _ := v.(string); s == "" || s == "vip" || s == "fraud-watch" {
			t.Fatalf("tags[%d] should be ciphertext, got %v", i, v)
		}
	}
	if err := gdpr.Unprotect(data, sch, c); err != nil {
		t.Fatalf("Unprotect: %v", err)
	}
	tags := data["tags"].([]any)
	if tags[0] != "vip" || tags[1] != "fraud-watch" {
		t.Fatalf("array-item round-trip failed: %v", tags)
	}
}

func TestUnprotect_NonJSONPlaintextErrors(t *testing.T) {
	// A cipher whose Decrypt returns bytes that are NOT valid JSON: decryptLeaf must surface
	// ErrDecrypt rather than hand the handler garbage.
	c := garbageDecryptCipher{}
	sch := parseSchema(t, `{"type":"object","properties":{"email":{"type":"string","x-gdpr-sensitive":true}}}`)
	// The value must be a string so decryptLeaf attempts a decrypt.
	data := map[string]any{"email": base64.StdEncoding.EncodeToString([]byte("anything"))}

	err := gdpr.Unprotect(data, sch, c)
	if err == nil || !errors.Is(err, gdpr.ErrDecrypt) {
		t.Fatalf("non-JSON plaintext should yield ErrDecrypt, got %v", err)
	}
}

// garbageDecryptCipher "decrypts" to non-JSON bytes, to exercise decryptLeaf's JSON-decode guard.
type garbageDecryptCipher struct{}

func (garbageDecryptCipher) Encrypt(pt []byte) (string, error) {
	return base64.StdEncoding.EncodeToString(pt), nil
}
func (garbageDecryptCipher) Decrypt(string) ([]byte, error) { return []byte("not json{"), nil }

// ensure the schema import is used even if the file's other helpers change.
var _ = schema.Parse
