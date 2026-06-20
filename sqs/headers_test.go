package sqs

import (
	"context"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

const traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

// TestPublishWithHeadersCarriesTraceparent proves the out-of-band traceparent rides
// as a String MessageAttribute beside the unchanged envelope body, alongside the
// contract bq-* attributes.
func TestPublishWithHeadersCarriesTraceparent(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))

	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": 1}, babelqueue.WithQueue("orders"))
	body, _ := env.Encode()
	if err := tr.PublishWithHeaders(context.Background(), "orders", string(body),
		map[string]string{"traceparent": traceparent, "tracestate": "vendor=v"}); err != nil {
		t.Fatalf("PublishWithHeaders: %v", err)
	}

	sent := fake.sent[0]
	if got := aws.ToString(sent.MessageBody); got != string(body) {
		t.Errorf("body changed: header propagation must not touch the body")
	}
	if got := attrValue(sent, "traceparent"); got != traceparent {
		t.Errorf("traceparent attribute = %q, want %q", got, traceparent)
	}
	if got := attrValue(sent, "tracestate"); got != "vendor=v" {
		t.Errorf("tracestate attribute = %q", got)
	}
	if dt := aws.ToString(sent.MessageAttributes["traceparent"].DataType); dt != "String" {
		t.Errorf("traceparent DataType = %q, want String", dt)
	}
	// The contract bq-* projection still rides beside the header.
	if got := attrValue(sent, "bq-trace-id"); got != env.TraceID {
		t.Errorf("contract bq-trace-id missing/changed: %q", got)
	}
}

// TestMergeAttributesDoesNotClobberContract proves a header keyed like a contract
// attribute can never overwrite the real bq-* projection.
func TestMergeAttributesDoesNotClobberContract(t *testing.T) {
	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1})
	body, _ := env.Encode()
	base := attributes(string(body))

	merged := mergeAttributes(base, map[string]string{
		"bq-trace-id": "ATTACKER", // collides with a contract attribute
		"traceparent": traceparent,
		"":            "blank-key",
		"empty-val":   "",
	})

	if got := aws.ToString(merged["bq-trace-id"].StringValue); got != env.TraceID {
		t.Errorf("contract bq-trace-id clobbered: got %q, want %q", got, env.TraceID)
	}
	if got := aws.ToString(merged["traceparent"].StringValue); got != traceparent {
		t.Errorf("traceparent not merged: %q", got)
	}
	if _, ok := merged[""]; ok {
		t.Error("blank header key must be skipped")
	}
	if _, ok := merged["empty-val"]; ok {
		t.Error("empty header value must be skipped")
	}
}

// TestMergeAttributesRespectsTenLimit proves the SQS 10-attribute ceiling is never
// exceeded even with many extra headers, and that the contract attributes (added
// first) are always preserved.
func TestMergeAttributesRespectsTenLimit(t *testing.T) {
	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1})
	body, _ := env.Encode()
	base := attributes(string(body)) // 6 contract attributes
	contractCount := len(base)

	headers := make(map[string]string, 20)
	for i := 0; i < 20; i++ {
		headers["extra-"+strconv.Itoa(i)] = "v"
	}
	merged := mergeAttributes(base, headers)

	if len(merged) > maxMessageAttributes {
		t.Fatalf("merged %d attributes, exceeds SQS limit %d", len(merged), maxMessageAttributes)
	}
	if len(merged) != maxMessageAttributes {
		t.Errorf("merged %d attributes, want the cap %d filled", len(merged), maxMessageAttributes)
	}
	// Every contract attribute must survive (they are added before the extras).
	for k := range attributes(string(body)) {
		if _, ok := merged[k]; !ok {
			t.Errorf("contract attribute %q dropped under the cap", k)
		}
	}
	if contractCount >= maxMessageAttributes {
		t.Skip("fixture already at the cap; extra-header bounding not exercised")
	}
}

// TestMergeAttributesNilHeadersUnchanged proves no extra headers leaves the contract
// projection byte-for-byte (Publish path unchanged).
func TestMergeAttributesNilHeadersUnchanged(t *testing.T) {
	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1})
	body, _ := env.Encode()
	base := attributes(string(body))
	if got := mergeAttributes(base, nil); len(got) != len(base) {
		t.Errorf("nil headers changed attribute count: %d -> %d", len(base), len(got))
	}
}

func TestHeadersFromAttributes(t *testing.T) {
	if got := headersFromAttributes(nil); got != nil {
		t.Errorf("nil attributes must surface nil, got %v", got)
	}
	if got := headersFromAttributes(map[string]types.MessageAttributeValue{}); got != nil {
		t.Errorf("empty attributes must surface nil, got %v", got)
	}

	str := func(v string) types.MessageAttributeValue {
		return types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
	}
	got := headersFromAttributes(map[string]types.MessageAttributeValue{
		"traceparent": str(traceparent),
		"bq-trace-id": str("t-123"),
		"empty":       str(""), // skipped
	})
	if got["traceparent"] != traceparent {
		t.Errorf("traceparent = %q", got["traceparent"])
	}
	if got["bq-trace-id"] != "t-123" {
		t.Errorf("bq-trace-id = %q", got["bq-trace-id"])
	}
	if _, ok := got["empty"]; ok {
		t.Error("empty-valued attribute must be skipped")
	}
}

// TestHeaderRoundTripThroughFakeBroker proves a traceparent published via
// PublishWithHeaders is surfaced on ReceivedMessage.Headers after a Pop, fully via
// the fake SQS client (no AWS).
func TestHeaderRoundTripThroughFakeBroker(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))

	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1}, babelqueue.WithQueue("orders"))
	body, _ := env.Encode()
	if err := tr.PublishWithHeaders(context.Background(), "orders", string(body),
		map[string]string{"traceparent": traceparent}); err != nil {
		t.Fatalf("PublishWithHeaders: %v", err)
	}

	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil || msg == nil {
		t.Fatalf("Pop: msg=%v err=%v", msg, err)
	}
	if msg.Headers["traceparent"] != traceparent {
		t.Fatalf("round-trip lost traceparent: got %q, want %q", msg.Headers["traceparent"], traceparent)
	}
}

var _ babelqueue.HeaderPublisher = (*Transport)(nil)
