package redis

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

// assertPoisonRecord checks that record is the ADR-0035 poison DLQ record for body: a byte splice
// when body is a strict JSON object without a dead_letter key, otherwise the raw wrapper (base64
// plus raw_encoding for invalid UTF-8). The dead_letter block must carry reason "poison" and the
// source queue.
func assertPoisonRecord(t *testing.T, record, body, queue string) {
	t.Helper()
	var got struct {
		Raw         *string `json:"raw"`
		RawEncoding *string `json:"raw_encoding"`
		DeadLetter  struct {
			Reason        string `json:"reason"`
			OriginalQueue string `json:"original_queue"`
		} `json:"dead_letter"`
	}
	if err := json.Unmarshal([]byte(record), &got); err != nil {
		t.Fatalf("record %q is not valid JSON: %v", record, err)
	}
	if got.DeadLetter.Reason != "poison" || got.DeadLetter.OriginalQueue != queue {
		t.Fatalf("dead_letter = %+v; want reason poison, original_queue %q (record %q)", got.DeadLetter, queue, record)
	}
	var members map[string]json.RawMessage
	objectBody := utf8.ValidString(body) && json.Unmarshal([]byte(body), &members) == nil
	if objectBody {
		_, hasDeadLetter := members["dead_letter"]
		objectBody = !hasDeadLetter
	}
	switch {
	case !utf8.ValidString(body):
		if got.Raw == nil || got.RawEncoding == nil || *got.RawEncoding != "base64" {
			t.Fatalf("record %q: want raw + raw_encoding base64 for invalid UTF-8", record)
		}
		decoded, err := base64.StdEncoding.DecodeString(*got.Raw)
		if err != nil || string(decoded) != body {
			t.Fatalf("record %q: raw does not decode to the exact body bytes (%v)", record, err)
		}
	case objectBody:
		if got.Raw != nil || !strings.HasPrefix(record, strings.TrimSuffix(body, "}")) {
			t.Fatalf("record %q: want the body spliced byte for byte", record)
		}
	default:
		if got.Raw == nil || *got.Raw != body || got.RawEncoding != nil {
			t.Fatalf("record %q: want a plain raw wrapper of the body", record)
		}
	}
}
