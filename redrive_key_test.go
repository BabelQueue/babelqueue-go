package babelqueue

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// keyedHandle stands in for a broker-native delivery handle that carries a record key.
type keyedHandle struct{ key []byte }

type keyedWrite struct {
	queue string
	body  string
	key   []byte
	from  *ReceivedMessage
}

// keyedTransport is a fake KeyedRepublisher transport: Pop hands out messages whose Handle
// carries a key, and PublishFrom records the key it was asked to carry.
type keyedTransport struct {
	pending  map[string][]*ReceivedMessage
	writes   []keyedWrite
	plain    []string // queues written through plain Publish
	failFrom func(queue string) error
}

func (k *keyedTransport) Publish(_ context.Context, queue, body string) error {
	k.plain = append(k.plain, queue)
	return nil
}

func (k *keyedTransport) Pop(_ context.Context, queue string, _ time.Duration) (*ReceivedMessage, error) {
	q := k.pending[queue]
	if len(q) == 0 {
		return nil, nil
	}
	k.pending[queue] = q[1:]
	return q[0], nil
}

func (k *keyedTransport) Ack(context.Context, *ReceivedMessage) error { return nil }

func (k *keyedTransport) PublishFrom(_ context.Context, queue, body string, from *ReceivedMessage) error {
	if k.failFrom != nil {
		if err := k.failFrom(queue); err != nil {
			return err
		}
	}
	var key []byte
	if h, ok := from.Handle.(keyedHandle); ok {
		key = h.key
	}
	k.writes = append(k.writes, keyedWrite{queue: queue, body: body, key: key, from: from})
	return nil
}

func dlqBody(t *testing.T) string {
	t.Helper()
	env, err := Make("urn:babel:orders:created", map[string]any{"order_id": 1}, WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Annotate(env, "failed", "orders", 3, errors.New("boom")).Encode()
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func sameKey(got, want []byte) bool {
	return (got == nil) == (want == nil) && bytes.Equal(got, want)
}

// A redrive carries the DLQ record's key byte-for-byte (null stays null, empty stays empty).
func TestRedriveCarriesRecordKey(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  []byte
	}{
		{"keyed", []byte("order-1042")},
		{"null", nil},
		{"empty", []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := &keyedTransport{pending: map[string][]*ReceivedMessage{
				"orders.dlq": {{Body: dlqBody(t), Queue: "orders.dlq", Handle: keyedHandle{key: tc.key}}},
			}}
			res, err := Redrive(context.Background(), tr, "orders.dlq", RedriveOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if res.Redriven != 1 || len(tr.writes) != 1 || tr.writes[0].queue != "orders" {
				t.Fatalf("redriven=%d writes=%+v; want one write to orders", res.Redriven, tr.writes)
			}
			if !sameKey(tr.writes[0].key, tc.key) {
				t.Fatalf("redriven key = %#v, want %#v", tr.writes[0].key, tc.key)
			}
			if len(tr.plain) != 0 {
				t.Fatalf("plain Publish used: %v", tr.plain)
			}
		})
	}
}

// Every restore path (dry run, unselected, undecodable, failed redrive write) writes the
// message back to the DLQ with its key.
func TestRedriveRestoreCarriesRecordKey(t *testing.T) {
	key := []byte("order-1042")
	newTr := func(body string) *keyedTransport {
		return &keyedTransport{pending: map[string][]*ReceivedMessage{
			"orders.dlq": {{Body: body, Queue: "orders.dlq", Handle: keyedHandle{key: key}}},
		}}
	}
	cases := []struct {
		name    string
		body    string
		opts    RedriveOptions
		fail    func(string) error
		wantErr bool
	}{
		{name: "dry run", body: dlqBody(t), opts: RedriveOptions{DryRun: true}},
		{name: "unselected", body: dlqBody(t), opts: RedriveOptions{Select: func(Envelope) bool { return false }}},
		{name: "undecodable", body: "not json"},
		{
			name: "failed redrive write", body: dlqBody(t), wantErr: true,
			fail: func(q string) error {
				if q == "orders" {
					return errors.New("broker down")
				}
				return nil
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := newTr(tc.body)
			tr.failFrom = tc.fail
			_, err := Redrive(context.Background(), tr, "orders.dlq", tc.opts)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if len(tr.writes) != 1 || tr.writes[0].queue != "orders.dlq" || !sameKey(tr.writes[0].key, key) {
				t.Fatalf("writes = %+v; want one restore to orders.dlq with key %q", tr.writes, key)
			}
		})
	}
}

// A transport without KeyedRepublisher behaves exactly as before: plain publish, route kept.
func TestRedriveWithoutKeyedRepublisherIsPlain(t *testing.T) {
	tr := NewInMemoryTransport()
	deadLettered(t, tr, "orders.dlq", "urn:babel:orders:created", "orders", map[string]any{"order_id": 1})
	res, err := Redrive(context.Background(), tr, "orders.dlq", RedriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 1 {
		t.Fatalf("redriven = %d, want 1", res.Redriven)
	}
	if got := drain(t, tr, "orders"); len(got) != 1 {
		t.Fatalf("orders has %d messages, want 1", len(got))
	}
}
