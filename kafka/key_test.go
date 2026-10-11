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

// The App's own re-publishes (retry, envelope DLQ, poison DLQ, version-gated DLQ) carry the
// consumed record's key byte-for-byte: a null key stays null, an empty key stays empty (§6.5).
func TestRepublishPreservesRecordKey(t *testing.T) {
	keys := []struct {
		name string
		key  []byte
	}{
		{"keyed", []byte("order-1042")},
		{"null", nil},
		{"empty", []byte{}},
	}
	paths := []struct {
		name      string
		body      func(t *testing.T) string
		headers   []kafkago.Header
		opts      []babelqueue.AppOption
		wantTopic string
	}{
		{
			name:      "retry",
			body:      func(t *testing.T) string { return sampleBody(t, 0) },
			opts:      []babelqueue.AppOption{babelqueue.WithMaxAttempts(3)},
			wantTopic: "orders",
		},
		{
			name:      "envelope DLQ",
			body:      func(t *testing.T) string { return sampleBody(t, 0) },
			opts:      []babelqueue.AppOption{babelqueue.WithMaxAttempts(1), babelqueue.WithDeadLetter(true)},
			wantTopic: "orders.dlq",
		},
		{
			name:      "raw poison DLQ",
			body:      func(*testing.T) string { return "not json" },
			opts:      []babelqueue.AppOption{babelqueue.WithDeadLetter(true)},
			wantTopic: "orders.dlq",
		},
		{
			name:      "version-gated DLQ",
			body:      func(*testing.T) string { return "not json" },
			headers:   []kafkago.Header{{Key: "bq-schema-version", Value: []byte("2")}},
			opts:      []babelqueue.AppOption{babelqueue.WithDeadLetter(true)},
			wantTopic: "orders.dlq",
		},
	}

	for _, p := range paths {
		for _, k := range keys {
			t.Run(p.name+"/"+k.name, func(t *testing.T) {
				rec := kafkago.Message{Topic: "orders", Key: k.key, Value: []byte(p.body(t)), Headers: p.headers}
				fr := &fakeReader{messages: []kafkago.Message{rec}}
				fw := &fakeWriter{}
				tr, err := New(WithWriter(fw), WithReaderFactory(func(string) Reader { return fr }),
					WithMaxWaitTime(20*time.Millisecond))
				if err != nil {
					t.Fatal(err)
				}
				app := babelqueue.NewApp(tr, append([]babelqueue.AppOption{babelqueue.WithRewindPause(0)}, p.opts...)...)
				app.Handle(urn, func(context.Context, babelqueue.Envelope) error { return errors.New("boom") })

				if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
					t.Fatal(err)
				}
				if len(fw.sent) != 1 || fw.sent[0].Topic != p.wantTopic {
					t.Fatalf("writes = %+v; want exactly one record on %q", fw.sent, p.wantTopic)
				}
				got := fw.sent[0].Key
				if (got == nil) != (k.key == nil) || !bytes.Equal(got, k.key) {
					t.Fatalf("republished key = %#v (nil=%v), want %#v (nil=%v)", got, got == nil, k.key, k.key == nil)
				}
			})
		}
	}
}

// A message the transport did not reserve (no kafka-go handle) republishes without a key.
func TestRepublishWithoutHandleHasNoKey(t *testing.T) {
	fw := &fakeWriter{}
	tr := newTransport(t, fw, &fakeReader{})
	err := tr.PublishFrom(context.Background(), "orders", sampleBody(t, 0), &babelqueue.ReceivedMessage{Queue: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fw.sent) != 1 || fw.sent[0].Key != nil {
		t.Fatalf("writes = %+v; want one record with a nil key", fw.sent)
	}
}
