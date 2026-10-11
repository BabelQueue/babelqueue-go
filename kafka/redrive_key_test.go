package kafka

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	kafkago "github.com/segmentio/kafka-go"
)

func dlqRecord(t *testing.T, key []byte) kafkago.Message {
	t.Helper()
	env, err := babelqueue.Make(urn, map[string]any{"order_id": 1042}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := babelqueue.Annotate(env, "failed", "orders", 3, errors.New("boom")).Encode()
	if err != nil {
		t.Fatal(err)
	}
	return kafkago.Message{Topic: "orders.dlq", Key: key, Value: b}
}

// Redrive (and the dry-run restore) over the Kafka transport carries the DLQ record's key
// byte-for-byte: a null key stays null, an empty key stays empty (§6.5).
func TestRedrivePreservesRecordKey(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  []byte
	}{
		{"keyed", []byte("order-1042")},
		{"null", nil},
		{"empty", []byte{}},
	} {
		for _, dry := range []bool{false, true} {
			name := tc.name + "/redrive"
			wantTopic := "orders"
			if dry {
				name = tc.name + "/restore"
				wantTopic = "orders.dlq"
			}
			t.Run(name, func(t *testing.T) {
				fr := &fakeReader{messages: []kafkago.Message{dlqRecord(t, tc.key)}}
				fw := &fakeWriter{}
				tr, err := New(WithWriter(fw), WithReaderFactory(func(string) Reader { return fr }))
				if err != nil {
					t.Fatal(err)
				}
				_, err = babelqueue.Redrive(context.Background(), tr, "orders.dlq",
					babelqueue.RedriveOptions{DryRun: dry, Timeout: 20 * time.Millisecond})
				if err != nil {
					t.Fatal(err)
				}
				if len(fw.sent) != 1 || fw.sent[0].Topic != wantTopic {
					t.Fatalf("writes = %+v; want exactly one record on %q", fw.sent, wantTopic)
				}
				got := fw.sent[0].Key
				if (got == nil) != (tc.key == nil) || !bytes.Equal(got, tc.key) {
					t.Fatalf("key = %#v (nil=%v), want %#v (nil=%v)", got, got == nil, tc.key, tc.key == nil)
				}
			})
		}
	}
}

// rejectingWriter rejects every write, standing in for a broker that is unreachable at restore time.
type rejectingWriter struct{ calls int }

func (f *rejectingWriter) WriteMessages(context.Context, ...kafkago.Message) error {
	f.calls++
	return errors.New("broker unavailable")
}

func redriveOnce(t *testing.T, w Writer, rec kafkago.Message, dry bool) (*fakeReader, error) {
	t.Helper()
	fr := &fakeReader{messages: []kafkago.Message{rec}}
	tr, err := New(WithWriter(w), WithReaderFactory(func(string) Reader { return fr }))
	if err != nil {
		t.Fatal(err)
	}
	_, rerr := babelqueue.Redrive(context.Background(), tr, rec.Topic,
		babelqueue.RedriveOptions{DryRun: dry, Timeout: 20 * time.Millisecond})
	return fr, rerr
}

// A restore writes the original record back verbatim: key, headers and value bytes (including a
// value that is not valid UTF-8, and a null value) are preserved, nothing is re-projected from the
// body, and the timestamp is "now" rather than the envelope's created_at.
func TestRestoreIsVerbatim(t *testing.T) {
	hdrs := []kafkago.Header{
		{Key: "traceparent", Value: []byte("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")},
		{Key: "bq-schema-version", Value: []byte("2")},
	}
	valid := dlqRecord(t, []byte("k"))

	for _, tc := range []struct {
		name  string
		value []byte
		dry   bool
	}{
		{"invalid-utf8", []byte{0xFF, 0xFE, 0x01}, false},
		{"null-value", nil, false},
		{"dry-run-envelope", valid.Value, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := kafkago.Message{Topic: "orders.dlq", Key: []byte("k"), Value: tc.value, Headers: hdrs}
			fw := &fakeWriter{}
			before := time.Now()
			fr, err := redriveOnce(t, fw, rec, tc.dry)
			if err != nil {
				t.Fatal(err)
			}
			if len(fw.sent) != 1 {
				t.Fatalf("want 1 restore write, got %d", len(fw.sent))
			}
			got := fw.sent[0]
			if got.Topic != "orders.dlq" || !bytes.Equal(got.Key, []byte("k")) {
				t.Fatalf("topic/key not preserved: %q %q", got.Topic, got.Key)
			}
			if !bytes.Equal(got.Value, tc.value) || (tc.value == nil) != (got.Value == nil) {
				t.Fatalf("value not byte-for-byte: got %v want %v", got.Value, tc.value)
			}
			if len(got.Headers) != len(hdrs) {
				t.Fatalf("headers not preserved verbatim: %v", got.Headers)
			}
			for i, h := range hdrs {
				if got.Headers[i].Key != h.Key || !bytes.Equal(got.Headers[i].Value, h.Value) {
					t.Fatalf("header %d changed: %v", i, got.Headers[i])
				}
			}
			if got.Time.Before(before) {
				t.Fatalf("restore timestamp %v is older than the restore itself (%v)", got.Time, before)
			}
			if len(fr.committed) != 1 {
				t.Fatalf("want the restored record committed once, got %d", len(fr.committed))
			}
		})
	}
}

// A failed restore write must not commit the offset, so the record is redelivered instead of lost.
func TestFailedRestoreDoesNotCommit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value []byte
		dry   bool
	}{
		{"invalid-utf8", []byte{0xFF, 0xFE, 0x01}, false},
		{"dry-run-envelope", dlqRecord(t, nil).Value, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := kafkago.Message{Topic: "orders.dlq", Value: tc.value}
			fw := &rejectingWriter{}
			fr, err := redriveOnce(t, fw, rec, tc.dry)
			if err == nil {
				t.Fatal("want the restore error surfaced, got nil")
			}
			if fw.calls != 1 {
				t.Fatalf("want 1 write attempt, got %d", fw.calls)
			}
			if len(fr.committed) != 0 {
				t.Fatalf("offset committed despite a failed restore: %d commit(s)", len(fr.committed))
			}
		})
	}
}

// A redrive to the work topic is not a restore: it keeps the §6 header projection and the
// created_at timestamp, and still carries the record key.
func TestRedriveToWorkTopicStillProjectsHeaders(t *testing.T) {
	rec := dlqRecord(t, []byte("order-1042"))
	rec.Headers = []kafkago.Header{{Key: "traceparent", Value: []byte("stale")}}
	fw := &fakeWriter{}
	fr, err := redriveOnce(t, fw, rec, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(fw.sent) != 1 || fw.sent[0].Topic != "orders" {
		t.Fatalf("want one write to the work topic, got %+v", fw.sent)
	}
	got := fw.sent[0]
	if !bytes.Equal(got.Key, []byte("order-1042")) {
		t.Fatalf("key lost: %q", got.Key)
	}
	var hasJob bool
	for _, h := range got.Headers {
		if h.Key == "traceparent" {
			t.Fatalf("original headers must not be copied on a redrive: %v", got.Headers)
		}
		if h.Key == "bq-job" {
			hasJob = true
		}
	}
	if !hasJob {
		t.Fatalf("§6 headers not re-projected: %v", got.Headers)
	}
	if got.Time.IsZero() {
		t.Fatalf("created_at timestamp missing")
	}
	if len(fr.committed) != 1 {
		t.Fatalf("want 1 commit, got %d", len(fr.committed))
	}
}

// A redrive whose restore write back to the DLQ fails must not commit (a Kafka commit is
// cumulative, so a later commit would move the group past the unrestored record): the transport
// rewinds, nothing is committed, and the next run sees the same record again.
func TestRedriveFailedRestoreRewindsAndNeverCommitsPastTheRecord(t *testing.T) {
	log := &offsetLog{records: []kafkago.Message{dlqRecord(t, []byte("k0")), dlqRecord(t, []byte("k1"))}}
	for i := range log.records {
		log.records[i].Offset = int64(i)
	}
	w := &dlqWriter{failDLQ: true}
	tr, err := New(WithWriter(w), WithReaderFactory(func(string) Reader { return log.newReader() }))
	if err != nil {
		t.Fatal(err)
	}
	opts := babelqueue.RedriveOptions{DryRun: true, Timeout: 20 * time.Millisecond}

	if _, err := babelqueue.Redrive(context.Background(), tr, "orders.dlq", opts); err == nil {
		t.Fatal("expected the failed restore write to surface")
	}
	if log.closed != 1 {
		t.Fatalf("readers closed = %d, want 1 (the failed restore must rewind)", log.closed)
	}
	if len(log.commits) != 0 || log.committed != 0 {
		t.Fatalf("commits = %v, committed = %d; nothing may be committed after a failed restore", log.commits, log.committed)
	}

	// The DLQ recovers: the very same records come back, and are now restored and committed.
	w.failDLQ = false
	log.fetched = nil
	res, err := babelqueue.Redrive(context.Background(), tr, "orders.dlq", opts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Skipped != 2 {
		t.Fatalf("res = %+v, want 2 restored", res)
	}
	if len(log.fetched) < 2 || log.fetched[0] != 0 || log.fetched[1] != 1 {
		t.Fatalf("fetched = %v, want to start again at [0 1]", log.fetched)
	}
	if log.committed != 2 {
		t.Fatalf("committed = %d, want 2", log.committed)
	}
}

// workTopicWriter records writes and rejects those to the work topic while failWork is set.
type workTopicWriter struct {
	failWork bool
	sent     []kafkago.Message
}

func (w *workTopicWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	for _, m := range msgs {
		if m.Topic == "orders" && w.failWork {
			return errors.New("work topic unavailable")
		}
		w.sent = append(w.sent, m)
	}
	return nil
}

// A failed redrive publish whose restore succeeds aborts the run: the record is restored and
// committed, then the transport rewinds so the rest of the drained batch (never processed) is
// redelivered. The rewind lands on the committed offset, i.e. just past the restored record, so
// the record is not seen (and restored) a second time.
func TestRedrivePublishFailureRewindsRemainingBatchWithoutDuplicatingTheRestoredRecord(t *testing.T) {
	log := &offsetLog{records: []kafkago.Message{dlqRecord(t, []byte("k0")), dlqRecord(t, []byte("k1"))}}
	for i := range log.records {
		log.records[i].Offset = int64(i)
	}
	w := &workTopicWriter{failWork: true}
	tr, err := New(WithWriter(w), WithReaderFactory(func(string) Reader { return log.newReader() }))
	if err != nil {
		t.Fatal(err)
	}
	opts := babelqueue.RedriveOptions{Timeout: 20 * time.Millisecond}

	if _, err := babelqueue.Redrive(context.Background(), tr, "orders.dlq", opts); err == nil {
		t.Fatal("expected the failed publish to surface")
	}
	if len(w.sent) != 1 || w.sent[0].Topic != "orders.dlq" {
		t.Fatalf("writes = %+v, want exactly one restore write to the DLQ", w.sent)
	}
	if len(log.commits) != 1 || log.commits[0] != 0 {
		t.Fatalf("commits = %v, want only the restored record (offset 0)", log.commits)
	}
	if log.closed != 1 {
		t.Fatalf("readers closed = %d, want 1 (the remaining batch must be rewound)", log.closed)
	}

	// The work topic recovers: the group resumes just past the restored record, so offset 0 is
	// not seen again and only offset 1 is redriven.
	w.failWork = false
	log.fetched = nil
	res, err := babelqueue.Redrive(context.Background(), tr, "orders.dlq", opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(log.fetched) == 0 || log.fetched[0] != 1 {
		t.Fatalf("fetched = %v, want to resume at offset 1", log.fetched)
	}
	for _, off := range log.fetched {
		if off == 0 {
			t.Fatalf("fetched = %v, the restored record must not come back", log.fetched)
		}
	}
	if res.Redriven != 1 {
		t.Fatalf("res = %+v, want 1 redriven", res)
	}
	var dlqWrites int
	for _, m := range w.sent {
		if m.Topic == "orders.dlq" {
			dlqWrites++
		}
	}
	if dlqWrites != 1 {
		t.Fatalf("DLQ writes = %d, want 1 (no duplicate of the restored record)", dlqWrites)
	}
}
