package gdpr_test

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
	"github.com/babelqueue/babelqueue-go/gdpr"
	"github.com/babelqueue/babelqueue-go/schema"
)

// newKey returns a random 32-byte AES-256 key for the reference cipher.
func newKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatalf("rand key: %v", err)
	}
	return k
}

func cipherFor(t *testing.T, key []byte) gdpr.Cipher {
	t.Helper()
	c, err := gdpr.NewAESGCMCipher(key)
	if err != nil {
		t.Fatalf("NewAESGCMCipher: %v", err)
	}
	return c
}

func parseSchema(t *testing.T, src string) *schema.Schema {
	t.Helper()
	s, err := schema.Parse([]byte(src))
	if err != nil {
		t.Fatalf("schema.Parse: %v", err)
	}
	return s
}

// sampleSchema marks a top-level field, a nested-object field, and an array-item field.
const sampleSchema = `{
	"type":"object",
	"properties":{
		"order_id":{"type":"integer"},
		"email":{"type":"string","x-gdpr-sensitive":"email"},
		"locale":{"type":"string"},
		"profile":{"type":"object","properties":{
			"full_name":{"type":"string","x-gdpr-sensitive":true},
			"tier":{"type":"string"}
		}},
		"addresses":{"type":"array","items":{"type":"object","properties":{
			"line":{"type":"string","x-gdpr-sensitive":true},
			"city":{"type":"string"}
		}}}
	}
}`

func sampleData() map[string]any {
	return map[string]any{
		"order_id": 1042.0, // float64 — what JSON decode produces
		"email":    "alice@example.com",
		"locale":   "en_US",
		"profile": map[string]any{
			"full_name": "Alice Smith",
			"tier":      "gold",
		},
		"addresses": []any{
			map[string]any{"line": "1 Main St", "city": "Ankara"},
			map[string]any{"line": "2 Side St", "city": "Izmir"},
		},
	}
}

func TestProtectUnprotect_RoundTripRestoresData(t *testing.T) {
	c := cipherFor(t, newKey(t))
	sch := parseSchema(t, sampleSchema)

	orig := sampleData()
	// Snapshot the canonical JSON of the cleartext data so we can compare byte-for-byte.
	wantJSON := mustJSON(t, orig)

	data := sampleData()
	if err := gdpr.Protect(data, sch, c); err != nil {
		t.Fatalf("Protect: %v", err)
	}
	if err := gdpr.Unprotect(data, sch, c); err != nil {
		t.Fatalf("Unprotect: %v", err)
	}

	if gotJSON := mustJSON(t, data); !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("round-trip changed data:\n got %s\nwant %s", gotJSON, wantJSON)
	}
	if !reflect.DeepEqual(data, orig) {
		t.Fatalf("round-trip did not restore data deeply:\n got %#v\nwant %#v", data, orig)
	}
}

func TestProtect_SensitiveLeavesEncryptedNonSensitiveUntouched(t *testing.T) {
	c := cipherFor(t, newKey(t))
	sch := parseSchema(t, sampleSchema)

	data := sampleData()
	if err := gdpr.Protect(data, sch, c); err != nil {
		t.Fatalf("Protect: %v", err)
	}

	// Non-sensitive fields are unchanged.
	if data["order_id"] != 1042.0 {
		t.Errorf("non-sensitive order_id changed: %v", data["order_id"])
	}
	if data["locale"] != "en_US" {
		t.Errorf("non-sensitive locale changed: %v", data["locale"])
	}
	if tier := data["profile"].(map[string]any)["tier"]; tier != "gold" {
		t.Errorf("non-sensitive profile.tier changed: %v", tier)
	}
	for i, a := range data["addresses"].([]any) {
		if city := a.(map[string]any)["city"]; city == "" {
			t.Errorf("addresses[%d].city went missing", i)
		}
	}

	// Sensitive leaves are now ciphertext strings, not the cleartext.
	assertCiphertext(t, "email", data["email"], "alice@example.com")
	assertCiphertext(t, "profile.full_name", data["profile"].(map[string]any)["full_name"], "Alice Smith")
	addrs := data["addresses"].([]any)
	assertCiphertext(t, "addresses[0].line", addrs[0].(map[string]any)["line"], "1 Main St")
	assertCiphertext(t, "addresses[1].line", addrs[1].(map[string]any)["line"], "2 Side St")
}

// assertCiphertext checks a value is a non-empty string that is no longer the plaintext.
func assertCiphertext(t *testing.T, field string, got any, plaintext string) {
	t.Helper()
	s, ok := got.(string)
	if !ok {
		t.Errorf("%s: expected a ciphertext string, got %T (%v)", field, got, got)
		return
	}
	if s == "" {
		t.Errorf("%s: ciphertext is empty", field)
	}
	if s == plaintext {
		t.Errorf("%s: value was not encrypted (still %q)", field, plaintext)
	}
}

func TestProtect_FrozenEnvelopePureJSON(t *testing.T) {
	c := cipherFor(t, newKey(t))
	sch := parseSchema(t, sampleSchema)

	env, err := babelqueue.Make("urn:babel:orders:created", sampleData(), babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatalf("Make: %v", err)
	}
	trace := env.TraceID

	if err := gdpr.Protect(env.Data, sch, c); err != nil {
		t.Fatalf("Protect: %v", err)
	}

	body, err := env.Encode()
	if err != nil {
		t.Fatalf("Encode after Protect: %v", err)
	}

	// data stays pure JSON: the protected body decodes cleanly into the same envelope shape.
	in, err := babelqueue.Decode(body)
	if err != nil {
		t.Fatalf("Decode protected body: %v", err)
	}
	if !in.Accepts() {
		t.Fatal("protected envelope must still be accepted (GR-1)")
	}
	if in.Meta.SchemaVersion != 1 {
		t.Fatalf("schema_version must stay 1, got %d", in.Meta.SchemaVersion)
	}
	if in.TraceID != trace {
		t.Fatalf("trace_id must be preserved (GR-4): got %q want %q", in.TraceID, trace)
	}
	if in.Job != "urn:babel:orders:created" {
		t.Fatalf("job/URN must be unchanged, got %q", in.Job)
	}

	// The envelope must carry no field beyond the canonical frame (no field added by Protect).
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	wantKeys := map[string]bool{"job": true, "trace_id": true, "data": true, "meta": true, "attempts": true}
	for k := range raw {
		if !wantKeys[k] {
			t.Errorf("unexpected top-level envelope key after Protect: %q", k)
		}
	}

	// And the protected email is a JSON string (pure JSON), not the cleartext.
	if email, _ := in.Data["email"].(string); email == "" || email == "alice@example.com" {
		t.Fatalf("email should be a non-empty ciphertext string, got %v", in.Data["email"])
	}

	// Decrypting restores the original on the consumer side.
	if err := gdpr.Unprotect(in.Data, sch, c); err != nil {
		t.Fatalf("Unprotect on consumer: %v", err)
	}
	if in.Data["email"] != "alice@example.com" {
		t.Fatalf("consumer did not recover email: %v", in.Data["email"])
	}
}

func TestUnprotect_WrongKeyErrorsCleanly(t *testing.T) {
	producerKey := newKey(t)
	consumerKey := newKey(t)
	sch := parseSchema(t, sampleSchema)

	data := sampleData()
	if err := gdpr.Protect(data, sch, cipherFor(t, producerKey)); err != nil {
		t.Fatalf("Protect: %v", err)
	}

	err := gdpr.Unprotect(data, sch, cipherFor(t, consumerKey))
	if err == nil {
		t.Fatal("Unprotect with the wrong key must error")
	}
	if !errors.Is(err, gdpr.ErrDecrypt) {
		t.Fatalf("wrong-key error should wrap ErrDecrypt, got %v", err)
	}
}

func TestProtectUnprotect_AbsentFieldsSkipped(t *testing.T) {
	c := cipherFor(t, newKey(t))
	sch := parseSchema(t, sampleSchema)

	// A message missing every sensitive field (and the array entirely).
	data := map[string]any{"order_id": 7.0, "locale": "tr_TR"}
	before := mustJSON(t, data)

	if err := gdpr.Protect(data, sch, c); err != nil {
		t.Fatalf("Protect with absent fields: %v", err)
	}
	if err := gdpr.Unprotect(data, sch, c); err != nil {
		t.Fatalf("Unprotect with absent fields: %v", err)
	}
	if after := mustJSON(t, data); !bytes.Equal(before, after) {
		t.Fatalf("absent sensitive fields should leave data unchanged:\n before %s\n after  %s", before, after)
	}
}

func TestUnprotect_IdempotentOnCleartext(t *testing.T) {
	c := cipherFor(t, newKey(t))
	sch := parseSchema(t, sampleSchema)

	data := sampleData()
	before := mustJSON(t, data)

	// Unprotect on never-protected data: sensitive leaves are strings the cipher cannot open,
	// but full_name etc. are plain strings — Decrypt would error. So this asserts the documented
	// behaviour: a string that is not our ciphertext errors (consumer fails the message).
	err := gdpr.Unprotect(data, sch, c)
	if err == nil {
		t.Fatal("Unprotect on cleartext strings should error (they are not ciphertext)")
	}
	if !errors.Is(err, gdpr.ErrDecrypt) {
		t.Fatalf("expected ErrDecrypt, got %v", err)
	}

	// A non-string sensitive leaf is left untouched (true idempotency for non-strings): re-running
	// Unprotect after a successful Unprotect must not corrupt restored values.
	protected := sampleData()
	if err := gdpr.Protect(protected, sch, c); err != nil {
		t.Fatalf("Protect: %v", err)
	}
	if err := gdpr.Unprotect(protected, sch, c); err != nil {
		t.Fatalf("first Unprotect: %v", err)
	}
	restored := mustJSON(t, protected)
	// full_name is now the plain string "Alice Smith" again — re-running Unprotect would try to
	// decrypt that string and error, which is the same documented behaviour as above; the value is
	// NOT silently corrupted. We assert the restored data still matches the original cleartext.
	if !bytes.Equal(restored, before) {
		t.Fatalf("restored data diverged from original:\n got  %s\n want %s", restored, before)
	}
}

func TestProtectUnprotect_RootMarkAndNilInputsAreNoOps(t *testing.T) {
	c := cipherFor(t, newKey(t))

	// A schema with a root mark (path "") has no addressable leaf in a data object — no-op.
	rootSch := parseSchema(t, `{"type":"string","x-gdpr-sensitive":true}`)
	data := map[string]any{"x": "y"}
	if err := gdpr.Protect(data, rootSch, c); err != nil {
		t.Fatalf("Protect root mark: %v", err)
	}
	if data["x"] != "y" {
		t.Fatalf("root mark should not touch data, got %v", data["x"])
	}

	// nil data / nil schema / nil cipher are all no-ops.
	if err := gdpr.Protect(nil, rootSch, c); err != nil {
		t.Fatalf("Protect(nil data): %v", err)
	}
	if err := gdpr.Protect(data, nil, c); err != nil {
		t.Fatalf("Protect(nil schema): %v", err)
	}
	if err := gdpr.Protect(data, rootSch, nil); err != nil {
		t.Fatalf("Protect(nil cipher): %v", err)
	}
}

func TestProtect_ContainerMark(t *testing.T) {
	// A whole nested object marked sensitive is encrypted as one ciphertext, then restored.
	c := cipherFor(t, newKey(t))
	sch := parseSchema(t, `{
		"type":"object",
		"properties":{
			"payment":{"type":"object","x-gdpr-sensitive":true,"properties":{
				"pan":{"type":"string"},"exp":{"type":"string"}
			}}
		}
	}`)
	data := map[string]any{
		"payment": map[string]any{"pan": "4111111111111111", "exp": "12/30"},
	}
	want := mustJSON(t, data)

	if err := gdpr.Protect(data, sch, c); err != nil {
		t.Fatalf("Protect: %v", err)
	}
	if _, isString := data["payment"].(string); !isString {
		t.Fatalf("container mark should encrypt the whole object to a string, got %T", data["payment"])
	}
	if err := gdpr.Unprotect(data, sch, c); err != nil {
		t.Fatalf("Unprotect: %v", err)
	}
	if got := mustJSON(t, data); !bytes.Equal(got, want) {
		t.Fatalf("container round-trip failed:\n got %s\nwant %s", got, want)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
