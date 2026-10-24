package babelqueue

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type poisonDLQCase struct {
	Name       string `json:"name"`
	Body       string `json:"body"`
	BodyBase64 string `json:"body_base64"`
	Expect     struct {
		Shape         string          `json:"shape"`
		Record        json.RawMessage `json:"record"`
		SpliceLiteral []string        `json:"splice_literals"`
		SpliceKeys    []string        `json:"splice_keys"`
	} `json:"expect"`
}

// parseObject parses a JSON object keeping numbers as json.Number, so an out-of-range literal
// (1e400) does not fail the parse.
func parseObject(raw []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// topLevelKeys returns the distinct top-level member names of a JSON object, read token by token
// so a repeated member or an out-of-range number cannot trip it.
func topLevelKeys(t *testing.T, record []byte) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(record))
	dec.UseNumber()
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		t.Fatalf("record is not an object: %v", err)
	}
	seen := map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			t.Fatalf("read key: %v", err)
		}
		seen[tok.(string)] = true
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			t.Fatalf("read value of %q: %v", tok, err)
		}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// TestPoisonDLQConformance runs the `poison_dlq` section: the DLQ record written for a poison body
// has the ADR-0035 shape.
func TestPoisonDLQConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "conformance", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	var sec struct {
		OriginalQueue string          `json:"original_queue"`
		Cases         []poisonDLQCase `json:"cases"`
	}
	if err := json.Unmarshal(m["poison_dlq"], &sec); err != nil {
		t.Fatalf("poison_dlq section: %v", err)
	}
	if len(sec.Cases) == 0 {
		t.Fatal("poison_dlq section has no cases")
	}

	for _, c := range sec.Cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			body := c.Body
			if c.BodyBase64 != "" {
				b, err := base64.StdEncoding.DecodeString(c.BodyBase64)
				if err != nil {
					t.Fatalf("body_base64: %v", err)
				}
				body = string(b)
			}
			rec, err := poisonDeadLetterBody(&ReceivedMessage{Body: body, Queue: sec.OriginalQueue}, errors.New("poison"))
			if err != nil {
				t.Fatal(err)
			}

			wantRec, err := parseObject(c.Expect.Record)
			if err != nil {
				t.Fatalf("expect.record: %v", err)
			}

			// The record itself must always be a strict JSON object carrying dead_letter.
			got, err := parseObject(rec)
			if err != nil {
				t.Fatalf("record is not valid JSON: %v: %q", err, rec)
			}
			dl, _ := got["dead_letter"].(map[string]any)
			if dl == nil || dl["reason"] != "poison" || dl["original_queue"] != sec.OriginalQueue {
				t.Fatalf("dead_letter = %v, want reason poison and original_queue %q", dl, sec.OriginalQueue)
			}
			delete(got, "dead_letter")

			if c.Expect.Shape == "splice_or_wrapper" {
				if reflect.DeepEqual(got, wantRec) {
					return // the wrapper
				}
				text := string(rec)
				for _, lit := range c.Expect.SpliceLiteral {
					if !strings.Contains(text, lit) {
						t.Errorf("record does not contain %q verbatim: %s", lit, text)
					}
				}
				if _, ok := got["raw"]; ok {
					t.Errorf("spliced record has a top-level raw member: %s", text)
				}
				if _, ok := got["raw_encoding"]; ok {
					t.Errorf("spliced record has a top-level raw_encoding member: %s", text)
				}
				keys := topLevelKeys(t, rec)
				want := append(append([]string(nil), c.Expect.SpliceKeys...), "dead_letter")
				sort.Strings(want)
				if !reflect.DeepEqual(keys, want) {
					t.Errorf("top-level members = %v, want %v", keys, want)
				}
				return
			}
			if !reflect.DeepEqual(got, wantRec) {
				t.Fatalf("record without dead_letter = %v, want %v", got, wantRec)
			}
		})
	}
}

// A body that is not valid UTF-8 is carried as base64 with raw_encoding, never as U+FFFD.
func TestPoisonRecordInvalidUTF8IsBase64(t *testing.T) {
	body := "{\"a\":\"\xff\xfe\"}"
	rec, err := poisonDeadLetterBody(&ReceivedMessage{Body: body, Queue: "orders"}, errors.New("poison"))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(rec, &got); err != nil {
		t.Fatal(err)
	}
	if got["raw_encoding"] != "base64" || got["raw"] != base64.StdEncoding.EncodeToString([]byte(body)) {
		t.Fatalf("record = %s, want base64 raw with raw_encoding", rec)
	}
	if strings.ContainsRune(string(rec), '�') {
		t.Fatalf("record holds U+FFFD: %s", rec)
	}
}
