// Package gdpr is the RUNTIME half of ADR-0030: SDK-side field-level encryption of the data
// fields a registry declared x-gdpr-sensitive. The babelqueue-registry only DECLARES and AUDITS
// sensitivity (and offers one-way Mask for safe logging); this package ENFORCES it on the wire —
// a producer encrypts each marked leaf before publish, a consumer decrypts it after decode.
//
// It is the Go reference other SDKs mirror, so the contract is deliberately tight:
//
//   - The envelope stays FROZEN (GR-1). Protect mutates only VALUES inside data: a sensitive
//     leaf's value becomes a ciphertext STRING. It never adds, renames, removes or retypes an
//     envelope field; meta.schema_version stays 1; trace_id is untouched (GR-4). data remains
//     pure JSON (GR-3) — a JSON string is still pure JSON, so any SDK can carry the envelope even
//     without the key (it just can't read the protected fields). The which-fields-are-sensitive
//     fact lives in the schema, not the message, so nothing about the frame changes.
//   - Zero heavy dependencies (GR-7). The crypto is a caller-provided [Cipher] interface
//     (KMS/Vault/HSM/tokenisation); the bundled [AESGCMCipher] is stdlib-only. The core module
//     pulls no crypto/KMS dependency.
//
// The sensitive paths come from the SAME per-URN [schema.Schema] the produce/consume validation
// path already loads (via a schema.Provider) — the x-gdpr-sensitive marks ride on it
// (schema.Schema.SensitivePaths). Protect/Unprotect are standalone helpers the caller invokes,
// so it is strictly opt-in: a producer/consumer that never calls them behaves exactly as before.
//
// Typical wiring (producer):
//
//	env, _ := babelqueue.Make(urn, data, babelqueue.WithQueue("orders"))
//	sch, ok, _ := provider.Schema(env.URN())
//	if ok {
//	    _ = schema.Check(provider, env.URN(), env.Data) // optional: validate cleartext first
//	    _ = gdpr.Protect(env.Data, sch, cipher)          // encrypt marked leaves IN PLACE
//	}
//	body, _ := env.Encode()                              // ciphertext rides inside data
//
// and the inverse on the consumer, after Decode and before the handler reads env.Data:
//
//	in, _ := babelqueue.Decode(body)
//	sch, ok, _ := provider.Schema(in.URN())
//	if ok {
//	    _ = gdpr.Unprotect(in.Data, sch, cipher)         // decrypt marked leaves IN PLACE
//	}
//
// Validate cleartext BEFORE Protect / AFTER Unprotect — a schema that constrains a sensitive
// field (minLength, enum, …) would reject the ciphertext string otherwise.
package gdpr

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/babelqueue/babelqueue-go/schema"
)

// ErrDecrypt wraps any failure to restore a protected field on the consume side (a wrong key, a
// tampered/garbled ciphertext, or a value that is not the string Protect produced). Unprotect
// stops at the first such failure and returns it wrapped so it is distinguishable from a missing
// field (which is skipped, not an error). Detect it with errors.Is.
var ErrDecrypt = errors.New("babelqueue/gdpr: cannot decrypt a protected field")

// Protect encrypts, in place, every value in data located at a path the schema marked
// x-gdpr-sensitive — the producer-side step, run after building data and before Encode/publish.
// Each marked leaf's value is canonically JSON-encoded and replaced by cipher.Encrypt's
// ciphertext STRING; the envelope frame, non-sensitive fields, and key order are untouched (GR-1).
//
// A marked path that is absent from data is skipped (not an error) — schemas evolve and a message
// need not carry every optional field. data may be nil (no-op). A nil schema or one with no marks
// is a no-op (nothing is sensitive). Container marks (a whole object/array marked sensitive) are
// supported: the entire sub-value is encoded and encrypted as one ciphertext string.
//
// On any cipher error Protect returns it wrapped and leaves data in a partially-protected state;
// callers should treat a Protect error as fatal for that message (do not publish it).
func Protect(data map[string]any, sch *schema.Schema, cipher Cipher) error {
	return walk(data, sch, cipher, encryptLeaf)
}

// Unprotect is the consumer-side inverse of Protect: it decrypts, in place, every value in data at
// an x-gdpr-sensitive path, restoring the original JSON value byte-for-byte. Run it after Decode
// and before the handler reads data.
//
// An absent path is skipped. A leaf that is NOT a string — e.g. it was never protected, or this
// is a re-run after a successful Unprotect — is left as-is, so re-invoking Unprotect on already-
// cleartext data is safe (idempotent for non-string leaves). A string that the cipher cannot open
// (wrong key, tampered, or not a ciphertext) returns [ErrDecrypt], wrapped — the consumer should
// fail the message (retry / dead-letter) rather than process unreadable PII.
func Unprotect(data map[string]any, sch *schema.Schema, cipher Cipher) error {
	return walk(data, sch, cipher, decryptLeaf)
}

// leafOp transforms a single sensitive leaf value (encrypt or decrypt). It returns the new value,
// a found flag (false → the value was absent/skippable; the caller leaves the slot alone), and an
// error.
type leafOp func(value any, cipher Cipher) (newValue any, ok bool, err error)

// walk drives a leafOp over every x-gdpr-sensitive path the schema declares. It resolves each
// path against data itself (NOT by re-walking the schema over the value), so the operation touches
// exactly the declared leaves and nothing else — non-sensitive siblings are never read or copied.
func walk(data map[string]any, sch *schema.Schema, cipher Cipher, op leafOp) error {
	if data == nil || sch == nil || cipher == nil {
		return nil
	}
	for _, sp := range sch.SensitivePaths() {
		if err := applyAtPath(data, parsePath(sp.Path), cipher, op); err != nil {
			return err
		}
	}
	return nil
}

// segment is one step of a sensitive path: a named object key, optionally with array-descent.
// "addresses[].line" parses to [{key:"addresses", array:true}, {key:"line"}]. A root mark
// ("x-gdpr-sensitive" on the schema itself, path "") yields a single empty-key segment, which is
// not a meaningful data path for an envelope's data object and is skipped.
type segment struct {
	key   string
	array bool // this key holds an array; descend into every element before the next segment
}

// parsePath splits a SensitivePath.Path ("email", "profile.full_name", "addresses[].line") into
// segments. The "[]" marker binds to the segment it trails, signalling array descent.
func parsePath(path string) []segment {
	if path == "" {
		return nil
	}
	var segs []segment
	for _, part := range splitDots(path) {
		s := segment{key: part}
		if n := len(s.key); n >= 2 && s.key[n-2:] == "[]" {
			s.array = true
			s.key = s.key[:n-2]
		}
		segs = append(segs, s)
	}
	return segs
}

// splitDots splits on '.' without importing strings — keeps the dependency surface minimal and
// mirrors the registry's dotted-path convention.
func splitDots(path string) []string {
	var out []string
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			out = append(out, path[start:i])
			start = i + 1
		}
	}
	return append(out, path[start:])
}

// applyAtPath resolves segs against the current node and runs op on the leaf(s). It descends
// objects by key and, when a segment is an array, fans out over every element. An absent key or a
// type mismatch (a path that does not exist in this particular message) is skipped silently —
// schemas describe the union of possible shapes; a given message need not contain every field.
func applyAtPath(node any, segs []segment, cipher Cipher, op leafOp) error {
	if len(segs) == 0 {
		return nil // root mark or exhausted path with no leaf key — nothing addressable in data
	}
	seg := segs[0]
	obj, ok := node.(map[string]any)
	if !ok {
		return nil // expected an object here but the message has something else — skip
	}
	child, present := obj[seg.key]
	if !present {
		return nil // absent field — skip (not an error)
	}

	last := len(segs) == 1

	if seg.array {
		arr, ok := child.([]any)
		if !ok {
			return nil // declared array but message has a non-array — skip
		}
		for i, elem := range arr {
			if last {
				newVal, ok, err := op(elem, cipher)
				if err != nil {
					return err
				}
				if ok {
					arr[i] = newVal
				}
				continue
			}
			if err := applyAtPath(elem, segs[1:], cipher, op); err != nil {
				return err
			}
		}
		return nil
	}

	if last {
		newVal, ok, err := op(child, cipher)
		if err != nil {
			return err
		}
		if ok {
			obj[seg.key] = newVal
		}
		return nil
	}
	return applyAtPath(child, segs[1:], cipher, op)
}

// encryptLeaf canonically JSON-encodes one field value and replaces it with the cipher's
// ciphertext string. The JSON encoding is what makes the round-trip exact: Unprotect's
// json.Unmarshal restores the same decoded-JSON value (float64 for numbers, map[string]any for
// objects, …) that the envelope codec would have produced, so Protect→Unprotect is byte-for-byte.
func encryptLeaf(value any, cipher Cipher) (any, bool, error) {
	plaintext, err := json.Marshal(value)
	if err != nil {
		return nil, false, fmt.Errorf("babelqueue/gdpr: encode field for encryption: %w", err)
	}
	ct, err := cipher.Encrypt(plaintext)
	if err != nil {
		return nil, false, fmt.Errorf("babelqueue/gdpr: encrypt field: %w", err)
	}
	return ct, true, nil
}

// decryptLeaf reverses encryptLeaf. A non-string leaf is left untouched (ok=false) so Unprotect is
// safe to re-run on already-cleartext data; a string that fails to open or to JSON-decode yields
// ErrDecrypt so the consumer fails the message rather than handling unreadable PII.
func decryptLeaf(value any, cipher Cipher) (any, bool, error) {
	s, isString := value.(string)
	if !isString {
		// Not a ciphertext string (already cleartext, or never protected) — leave as-is.
		return nil, false, nil
	}
	plaintext, err := cipher.Decrypt(s)
	if err != nil {
		return nil, false, fmt.Errorf("%w: %v", ErrDecrypt, err)
	}
	var restored any
	if err := json.Unmarshal(plaintext, &restored); err != nil {
		return nil, false, fmt.Errorf("%w: decoded plaintext is not JSON: %v", ErrDecrypt, err)
	}
	return restored, true, nil
}
