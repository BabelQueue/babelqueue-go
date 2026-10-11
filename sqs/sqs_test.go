package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	neturl "net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

var errFake = errors.New("fake sqs error")

// requirePoisonDLQBody asserts got is the wrapped poison DLQ record for body: a body that is a
// JSON object is kept verbatim (its keys survive), anything else is carried as "raw"; either way
// a dead_letter block with reason "poison" and the source queue is added.
func requirePoisonDLQBody(t *testing.T, got, body string) {
	t.Helper()
	var rec map[string]any
	if err := json.Unmarshal([]byte(got), &rec); err != nil {
		t.Fatalf("%q: dead-letter body is not a JSON object: %v: %q", body, err, got)
	}
	var orig map[string]any
	if json.Unmarshal([]byte(body), &orig) == nil && orig != nil {
		for k, v := range orig {
			if rec[k] == nil && v != nil {
				t.Errorf("%q: key %q lost from the dead-letter record %q", body, k, got)
			}
		}
	} else if rec["raw"] != body {
		t.Errorf("%q: raw = %v, want the original body", body, rec["raw"])
	}
	dl, _ := rec["dead_letter"].(map[string]any)
	if dl == nil || dl["reason"] != "poison" || dl["original_queue"] != "orders" {
		t.Errorf("%q: dead_letter = %v, want reason poison, original_queue orders", body, dl)
	}
}

// fakeSQS is an in-memory API implementation — no AWS, no network.
type fakeSQS struct {
	mu          sync.Mutex
	visible     map[string][]types.Message // url -> queued messages
	inflight    map[string]string          // receiptHandle -> url
	sent        []*awssqs.SendMessageInput
	deleted     []string
	nextID      int
	getURLCalls int
	lastReceive *awssqs.ReceiveMessageInput
	held        map[string]types.Message // receiptHandle -> in-flight message
	visibility  []*awssqs.ChangeMessageVisibilityInput
	err         error // when set, every call returns it
	deleteErr   error // when set, DeleteMessage alone fails
}

func newFakeSQS() *fakeSQS {
	return &fakeSQS{visible: map[string][]types.Message{}, inflight: map[string]string{}}
}

func (f *fakeSQS) GetQueueUrl(_ context.Context, in *awssqs.GetQueueUrlInput, _ ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
	f.mu.Lock()
	f.getURLCalls++
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return &awssqs.GetQueueUrlOutput{QueueUrl: aws.String("http://fake/" + aws.ToString(in.QueueName))}, nil
}

func (f *fakeSQS) SendMessage(_ context.Context, in *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.sent = append(f.sent, in)
	url := aws.ToString(in.QueueUrl)
	f.nextID++
	handle := "rh-" + strconv.Itoa(f.nextID)
	f.visible[url] = append(f.visible[url], types.Message{
		Body:              in.MessageBody,
		MessageAttributes: in.MessageAttributes,
		ReceiptHandle:     aws.String(handle),
		Attributes:        map[string]string{"ApproximateReceiveCount": "1"},
	})
	return &awssqs.SendMessageOutput{MessageId: aws.String(handle)}, nil
}

func (f *fakeSQS) ReceiveMessage(_ context.Context, in *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastReceive = in
	if f.err != nil {
		return nil, f.err
	}
	url := aws.ToString(in.QueueUrl)
	q := f.visible[url]
	if len(q) == 0 {
		return &awssqs.ReceiveMessageOutput{}, nil
	}
	m := q[0]
	f.visible[url] = q[1:]
	f.inflight[aws.ToString(m.ReceiptHandle)] = url
	if f.held == nil {
		f.held = map[string]types.Message{}
	}
	f.held[aws.ToString(m.ReceiptHandle)] = m
	return &awssqs.ReceiveMessageOutput{Messages: []types.Message{m}}, nil
}

func (f *fakeSQS) DeleteMessage(_ context.Context, in *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	handle := aws.ToString(in.ReceiptHandle)
	f.deleted = append(f.deleted, handle)
	delete(f.inflight, handle)
	return &awssqs.DeleteMessageOutput{}, nil
}

// ChangeMessageVisibility records the call and, like SQS once the timeout elapses,
// makes the same message visible again with its receive count bumped by one.
func (f *fakeSQS) ChangeMessageVisibility(_ context.Context, in *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.visibility = append(f.visibility, in)
	handle := aws.ToString(in.ReceiptHandle)
	if m, ok := f.held[handle]; ok {
		delete(f.held, handle)
		delete(f.inflight, handle)
		rc, _ := strconv.Atoi(m.Attributes["ApproximateReceiveCount"])
		m.Attributes = map[string]string{"ApproximateReceiveCount": strconv.Itoa(rc + 1)}
		url := aws.ToString(in.QueueUrl)
		f.visible[url] = append(f.visible[url], m)
	}
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}

// seed pushes a raw message with a chosen ApproximateReceiveCount.
func (f *fakeSQS) seed(url, body string, receiveCount int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	f.visible[url] = append(f.visible[url], types.Message{
		Body:          aws.String(body),
		ReceiptHandle: aws.String("seed-" + strconv.Itoa(f.nextID)),
		Attributes:    map[string]string{"ApproximateReceiveCount": strconv.Itoa(receiveCount)},
	})
}

func attrValue(in *awssqs.SendMessageInput, key string) string {
	if in.MessageAttributes == nil {
		return ""
	}
	return aws.ToString(in.MessageAttributes[key].StringValue)
}

func TestPublishProjectsContractAttributes(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))

	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": 1042}, babelqueue.WithQueue("orders"))
	body, _ := env.Encode()
	if err := tr.Publish(context.Background(), "orders", string(body)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if len(fake.sent) != 1 {
		t.Fatalf("want 1 send, got %d", len(fake.sent))
	}
	sent := fake.sent[0]
	if got := aws.ToString(sent.QueueUrl); got != "http://fake/orders" {
		t.Errorf("queue url = %q", got)
	}
	if got := aws.ToString(sent.MessageBody); got != string(body) {
		t.Errorf("body not byte-identical: %q", got)
	}
	checks := map[string]string{
		"bq-job":            env.Job,
		"bq-trace-id":       env.TraceID,
		"bq-message-id":     env.Meta.ID,
		"bq-schema-version": "1",
		"bq-source-lang":    "go",
		"bq-created-at":     strconv.FormatInt(env.Meta.CreatedAt, 10),
	}
	for k, want := range checks {
		if got := attrValue(sent, k); got != want {
			t.Errorf("attribute %s = %q, want %q", k, got, want)
		}
	}
	// Type discipline: ids are String, counters are Number.
	if dt := aws.ToString(sent.MessageAttributes["bq-job"].DataType); dt != "String" {
		t.Errorf("bq-job DataType = %q", dt)
	}
	if dt := aws.ToString(sent.MessageAttributes["bq-schema-version"].DataType); dt != "Number" {
		t.Errorf("bq-schema-version DataType = %q", dt)
	}
}

func TestPopReconcilesAttemptsFromReceiveCount(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))

	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1}, babelqueue.WithQueue("orders"))
	body, _ := env.Encode()
	fake.seed("http://fake/orders", string(body), 3) // 3rd delivery → attempts must read 2

	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if msg == nil {
		t.Fatal("Pop returned nil")
	}
	got, _ := babelqueue.Decode([]byte(msg.Body))
	if got.Attempts != 2 {
		t.Errorf("attempts = %d, want 2 (ApproximateReceiveCount 3 − 1)", got.Attempts)
	}
}

func TestPopDoesNotLowerRuntimeAttempts(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))

	// Runtime already incremented to 5 (republished); a fresh SQS message has
	// ApproximateReceiveCount 1 → reconciliation must NOT lower it.
	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1})
	env.Attempts = 5
	body, _ := env.Encode()
	fake.seed("http://fake/default", string(body), 1)

	msg, _ := tr.Pop(context.Background(), "default", 0)
	got, _ := babelqueue.Decode([]byte(msg.Body))
	if got.Attempts != 5 {
		t.Errorf("attempts = %d, want 5 (must not be lowered)", got.Attempts)
	}
}

func TestPopEmptyReturnsNil(t *testing.T) {
	tr := NewWithClient(newFakeSQS(), WithQueueURLPrefix("http://fake"))
	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil || msg != nil {
		t.Fatalf("want (nil,nil), got (%v,%v)", msg, err)
	}
}

func TestAckDeletesByReceiptHandle(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))
	fake.seed("http://fake/orders", `{"job":"urn:x:y","trace_id":"t","data":{},"meta":{"schema_version":1},"attempts":0}`, 1)

	msg, _ := tr.Pop(context.Background(), "orders", 0)
	if err := tr.Ack(context.Background(), msg); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if len(fake.deleted) != 1 || fake.deleted[0] != msg.Handle.(string) {
		t.Errorf("deleted = %v, want [%v]", fake.deleted, msg.Handle)
	}
}

func TestFIFOSetsGroupAndDedup(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"), WithFIFO(true))

	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1}, babelqueue.WithQueue("orders.fifo"))
	body, _ := env.Encode()
	_ = tr.Publish(context.Background(), "orders.fifo", string(body))

	sent := fake.sent[0]
	if got := aws.ToString(sent.MessageGroupId); got != "orders.fifo" {
		t.Errorf("MessageGroupId = %q, want queue name", got)
	}
	if got := aws.ToString(sent.MessageDeduplicationId); got != env.Meta.ID {
		t.Errorf("MessageDeduplicationId = %q, want meta.id %q", got, env.Meta.ID)
	}
}

func TestRoundTripThroughApp(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))
	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))

	var seen babelqueue.Envelope
	app.Handle("urn:babel:orders:created", func(_ context.Context, env babelqueue.Envelope) error {
		seen = env
		return nil
	})

	id, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{"order_id": 7})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	n, err := app.Drain(context.Background(), "orders", 10)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if n != 1 {
		t.Fatalf("drained %d, want 1", n)
	}
	if seen.URN() != "urn:babel:orders:created" || seen.Meta.ID != id {
		t.Errorf("handler saw urn=%q id=%q, want urn:babel:orders:created / %q", seen.URN(), seen.Meta.ID, id)
	}
	if seen.Data["order_id"].(float64) != 7 {
		t.Errorf("data.order_id = %v, want 7", seen.Data["order_id"])
	}
	if len(fake.deleted) != 1 {
		t.Errorf("message not acked/deleted: %v", fake.deleted)
	}
}

func TestNewWithInjectedClientSkipsAWSConfig(t *testing.T) {
	fake := newFakeSQS()
	tr, err := New(context.Background(), WithClient(fake), WithRegion("eu-central-1"), WithEndpoint("http://localhost:4566"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if tr.client != fake {
		t.Error("injected client not used")
	}
}

func TestResolveURLViaGetQueueUrlAndCaches(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake) // no prefix -> GetQueueUrl path
	body := `{"job":"urn:x:y","trace_id":"t","data":{},"meta":{"schema_version":1,"lang":"go"},"attempts":0}`

	for i := 0; i < 3; i++ {
		if err := tr.Publish(context.Background(), "orders", body); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if got := aws.ToString(fake.sent[0].QueueUrl); got != "http://fake/orders" {
		t.Errorf("resolved url = %q", got)
	}
	if fake.getURLCalls != 1 {
		t.Errorf("GetQueueUrl called %d times, want 1 (cached)", fake.getURLCalls)
	}
}

func TestPopAppliesVisibilityAndWaitOptions(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"),
		WithVisibilityTimeout(45), WithWaitTimeSeconds(5))

	if _, err := tr.Pop(context.Background(), "orders", 30*time.Second); err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if fake.lastReceive.VisibilityTimeout != 45 {
		t.Errorf("VisibilityTimeout = %d, want 45", fake.lastReceive.VisibilityTimeout)
	}
	// timeout=30s clamps to 20, then WithWaitTimeSeconds(5) caps it to 5.
	if fake.lastReceive.WaitTimeSeconds != 5 {
		t.Errorf("WaitTimeSeconds = %d, want 5", fake.lastReceive.WaitTimeSeconds)
	}
	if len(fake.lastReceive.MessageAttributeNames) != 1 || fake.lastReceive.MessageAttributeNames[0] != "All" {
		t.Errorf("MessageAttributeNames = %v, want [All]", fake.lastReceive.MessageAttributeNames)
	}
}

func TestContentDedupOmitsDeduplicationID(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"),
		WithFIFO(true), WithContentDedup(true), WithMessageGroupID("grp"))

	body := `{"job":"urn:x:y","trace_id":"t","data":{},"meta":{"id":"m1","schema_version":1},"attempts":0}`
	_ = tr.Publish(context.Background(), "orders.fifo", body)

	sent := fake.sent[0]
	if aws.ToString(sent.MessageGroupId) != "grp" {
		t.Errorf("MessageGroupId = %q, want grp", aws.ToString(sent.MessageGroupId))
	}
	if sent.MessageDeduplicationId != nil {
		t.Errorf("MessageDeduplicationId set under content-dedup: %q", aws.ToString(sent.MessageDeduplicationId))
	}
}

func TestReconcileAttemptsIgnoresGarbageReceiveCount(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))
	body := `{"job":"urn:x:y","trace_id":"t","data":{},"meta":{"schema_version":1},"attempts":4}`
	fake.seed("http://fake/orders", body, 0) // receiveCount "0" -> <=1, no change
	fake.visible["http://fake/orders"][0].Attributes["ApproximateReceiveCount"] = "not-a-number"

	msg, _ := tr.Pop(context.Background(), "orders", 0)
	got, _ := babelqueue.Decode([]byte(msg.Body))
	if got.Attempts != 4 {
		t.Errorf("attempts = %d, want 4 (garbage receive-count ignored)", got.Attempts)
	}
}

func TestAttributesNilForUndecodableBody(t *testing.T) {
	if got := attributes("}{not json"); got != nil {
		t.Errorf("attributes(garbage) = %v, want nil", got)
	}
}

func TestAckNoopOnEmptyHandle(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))
	if err := tr.Ack(context.Background(), &babelqueue.ReceivedMessage{Queue: "orders", Handle: ""}); err != nil {
		t.Fatalf("Ack empty handle: %v", err)
	}
	if len(fake.deleted) != 0 {
		t.Errorf("empty handle should not delete: %v", fake.deleted)
	}
}

func TestErrorsPropagate(t *testing.T) {
	wantErr := errFake
	ctx := context.Background()

	// resolveURL via GetQueueUrl error (no prefix), surfaced through every op.
	failURL := NewWithClient(&fakeSQS{visible: map[string][]types.Message{}, inflight: map[string]string{}, err: wantErr})
	if err := failURL.Publish(ctx, "orders", `{"job":"u","trace_id":"t","data":{},"meta":{"schema_version":1}}`); err != wantErr {
		t.Errorf("Publish GetQueueUrl error = %v, want %v", err, wantErr)
	}
	if _, err := failURL.Pop(ctx, "orders", 0); err != wantErr {
		t.Errorf("Pop GetQueueUrl error = %v, want %v", err, wantErr)
	}
	if err := failURL.Ack(ctx, &babelqueue.ReceivedMessage{Queue: "orders", Handle: "h"}); err != wantErr {
		t.Errorf("Ack GetQueueUrl error = %v, want %v", err, wantErr)
	}

	// SendMessage / ReceiveMessage / DeleteMessage errors (prefix skips GetQueueUrl).
	fail := &fakeSQS{visible: map[string][]types.Message{}, inflight: map[string]string{}, err: wantErr}
	tr := NewWithClient(fail, WithQueueURLPrefix("http://fake"))
	if err := tr.Publish(ctx, "orders", `{"job":"u","trace_id":"t","data":{},"meta":{"schema_version":1}}`); err != wantErr {
		t.Errorf("Publish SendMessage error = %v, want %v", err, wantErr)
	}
	if _, err := tr.Pop(ctx, "orders", 0); err != wantErr {
		t.Errorf("Pop ReceiveMessage error = %v, want %v", err, wantErr)
	}
	if err := tr.Ack(ctx, &babelqueue.ReceivedMessage{Queue: "orders", Handle: "h"}); err != wantErr {
		t.Errorf("Ack DeleteMessage error = %v, want %v", err, wantErr)
	}
}

func TestReconcileLeavesUndecodableBody(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))
	fake.seed("http://fake/orders", "not-json", 3) // rc>1 but body won't decode
	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if msg.Body != "not-json" {
		t.Errorf("body = %q, want unchanged", msg.Body)
	}
}

var _ API = (*fakeSQS)(nil)

// apiOnly hides the fake's ChangeMessageVisibility, modelling a pre-existing API
// implementation without the VisibilityAPI capability.
type apiOnly struct{ API }

func TestReleaseChangesVisibilityAndDoesNotDelete(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))
	msg := &babelqueue.ReceivedMessage{Queue: "orders", Handle: "rh-9"}

	cases := []struct {
		delay time.Duration
		want  int32
	}{
		{0, 0},
		{-time.Second, 0},
		{30 * time.Second, 30},
		{1500 * time.Millisecond, 2},
		{13 * time.Hour, MaxVisibilityTimeoutSeconds},
	}
	for _, c := range cases {
		if err := tr.Release(context.Background(), msg, c.delay); err != nil {
			t.Fatalf("Release(%v): %v", c.delay, err)
		}
	}
	if len(fake.visibility) != len(cases) {
		t.Fatalf("ChangeMessageVisibility calls = %d, want %d", len(fake.visibility), len(cases))
	}
	for i, c := range cases {
		in := fake.visibility[i]
		if aws.ToString(in.QueueUrl) != "http://fake/orders" || aws.ToString(in.ReceiptHandle) != "rh-9" {
			t.Errorf("call %d: url=%q handle=%q", i, aws.ToString(in.QueueUrl), aws.ToString(in.ReceiptHandle))
		}
		if in.VisibilityTimeout != c.want {
			t.Errorf("delay %v: VisibilityTimeout = %d, want %d", c.delay, in.VisibilityTimeout, c.want)
		}
	}
	if len(fake.deleted) != 0 || len(fake.sent) != 0 {
		t.Errorf("release must not delete or re-send: deleted=%v sent=%d", fake.deleted, len(fake.sent))
	}
}

func TestReleaseUnsupportedWithoutVisibilityAPI(t *testing.T) {
	tr := NewWithClient(apiOnly{newFakeSQS()}, WithQueueURLPrefix("http://fake"))
	err := tr.Release(context.Background(), &babelqueue.ReceivedMessage{Queue: "orders", Handle: "h"}, time.Second)
	if !errors.Is(err, ErrReleaseUnsupported) {
		t.Fatalf("err = %v, want ErrReleaseUnsupported", err)
	}
	full := NewWithClient(newFakeSQS(), WithQueueURLPrefix("http://fake"))
	if err := full.Release(context.Background(), &babelqueue.ReceivedMessage{Queue: "orders"}, 0); !errors.Is(err, ErrReleaseUnsupported) {
		t.Fatalf("empty handle: err = %v, want ErrReleaseUnsupported", err)
	}
}

// TestAppRetryUsesChangeMessageVisibility drives the runtime end to end: each failed
// attempt is released in place with the configured backoff (no re-send, no delete);
// the receive count carries attempts until the message is dead-lettered.
func TestAppRetryUsesChangeMessageVisibility(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))
	app := babelqueue.NewApp(tr,
		babelqueue.WithDefaultQueue("orders"),
		babelqueue.WithMaxAttempts(3),
		babelqueue.WithDeadLetter(true),
		babelqueue.WithRetryBackoff(45*time.Second),
	)
	var attempts []int
	app.Handle("urn:babel:orders:created", func(_ context.Context, env babelqueue.Envelope) error {
		attempts = append(attempts, env.Attempts)
		return errors.New("boom")
	})
	if _, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{"order_id": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 10); err != nil {
		t.Fatal(err)
	}

	if want := []int{0, 1, 2}; !equalInts(attempts, want) {
		t.Errorf("attempts seen = %v, want %v (from ApproximateReceiveCount)", attempts, want)
	}
	if len(fake.visibility) != 2 {
		t.Fatalf("ChangeMessageVisibility calls = %d, want 2", len(fake.visibility))
	}
	for _, in := range fake.visibility {
		if in.VisibilityTimeout != 45 {
			t.Errorf("VisibilityTimeout = %d, want 45 (retry backoff)", in.VisibilityTimeout)
		}
	}
	// 1 original publish + 1 dead-letter send; no retry re-sends. One delete (the final ack).
	if len(fake.sent) != 2 || aws.ToString(fake.sent[1].QueueUrl) != "http://fake/orders.dlq" {
		t.Errorf("sends = %d (want 2, last to orders.dlq)", len(fake.sent))
	}
	if len(fake.deleted) != 1 {
		t.Errorf("deletes = %v, want exactly the final ack", fake.deleted)
	}
}

// An undecodable body can never succeed on retry, so with dead-lettering on it is moved to
// <queue>.dlq wrapped (original object or {"raw": <body>} plus a poison dead_letter block) on the
// first delivery: no ChangeMessageVisibility release, no retry, and a single delete as the final ack.
func TestAppUndecodableBodyIsDeadLetteredWrappedWithoutRetry(t *testing.T) {
	for _, body := range []string{
		`{"job":"urn:babel:orders:created","trace_id":"t","data":[1,2],"meta":{"schema_version":1},"attempts":0}`,
		`not json at all`,
	} {
		fake := newFakeSQS()
		tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))
		app := babelqueue.NewApp(tr,
			babelqueue.WithDefaultQueue("orders"),
			babelqueue.WithMaxAttempts(3),
			babelqueue.WithDeadLetter(true),
		)
		fake.seed("http://fake/orders", body, 1)

		n, err := app.Drain(context.Background(), "orders", 50)
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("%q: processed %d deliveries, want 1 (no retry)", body, n)
		}
		if len(fake.visibility) != 0 {
			t.Errorf("%q: ChangeMessageVisibility calls = %d, want 0", body, len(fake.visibility))
		}
		if len(fake.sent) != 1 || aws.ToString(fake.sent[0].QueueUrl) != "http://fake/orders.dlq" {
			t.Fatalf("%q: sends = %d, want exactly one to orders.dlq", body, len(fake.sent))
		}
		requirePoisonDLQBody(t, aws.ToString(fake.sent[0].MessageBody), body)
		if len(fake.deleted) != 1 {
			t.Errorf("%q: deletes = %v, want exactly the final ack", body, fake.deleted)
		}
	}
}

// A DeleteMessage failure after the handler succeeded is an ack failure, not a
// handler failure: the message is neither released, re-sent nor dead-lettered, and
// the error reaches WithAckErrorHandler.
func TestAppDeleteFailureAfterSuccessIsNotAHandlerFailure(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))
	var ackErrs []error
	app := babelqueue.NewApp(tr,
		babelqueue.WithDefaultQueue("orders"),
		babelqueue.WithDeadLetter(true),
		babelqueue.WithAckErrorHandler(func(_ context.Context, msg *babelqueue.ReceivedMessage, err error) {
			if msg == nil || msg.Handle == "" {
				t.Errorf("ack error handler got message %+v", msg)
			}
			ackErrs = append(ackErrs, err)
		}),
	)
	calls := 0
	app.Handle("urn:babel:orders:created", func(context.Context, babelqueue.Envelope) error {
		calls++
		return nil
	})
	if _, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{"order_id": 1}); err != nil {
		t.Fatal(err)
	}
	fake.deleteErr = errFake
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Errorf("handler calls = %d, want 1", calls)
	}
	if len(ackErrs) != 1 || !errors.Is(ackErrs[0], errFake) {
		t.Errorf("ack errors = %v, want [%v]", ackErrs, errFake)
	}
	if len(fake.visibility) != 0 {
		t.Errorf("a failed delete must not release: ChangeMessageVisibility calls = %d", len(fake.visibility))
	}
	if len(fake.sent) != 1 {
		t.Errorf("a failed delete must not re-send or dead-letter: sends = %d, want 1 (the publish)", len(fake.sent))
	}
}

func TestVisibilitySecondsDoesNotOverflow(t *testing.T) {
	if got := visibilitySeconds(time.Duration(math.MaxInt64)); got != MaxVisibilityTimeoutSeconds {
		t.Errorf("visibilitySeconds(MaxInt64) = %d, want %d", got, MaxVisibilityTimeoutSeconds)
	}
}

func TestAppUnknownURNReleaseUsesChangeMessageVisibility(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))
	app := babelqueue.NewApp(tr,
		babelqueue.WithDefaultQueue("orders"),
		babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease),
		babelqueue.WithUnknownURNReleaseDelay(5*time.Second),
	)
	if _, err := app.Publish(context.Background(), "urn:babel:nobody:listens", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if len(fake.visibility) != 1 || fake.visibility[0].VisibilityTimeout != 5 {
		t.Fatalf("ChangeMessageVisibility = %+v, want one call with 5s", fake.visibility)
	}
	if len(fake.deleted) != 0 || len(fake.sent) != 1 {
		t.Errorf("unknown-URN release must not delete or re-send: deleted=%v sent=%d", fake.deleted, len(fake.sent))
	}
}

func TestAppFallsBackToRepublishWithoutVisibilityAPI(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(apiOnly{fake}, WithQueueURLPrefix("http://fake"))
	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"), babelqueue.WithMaxAttempts(2))
	calls := 0
	app.Handle("urn:babel:orders:created", func(context.Context, babelqueue.Envelope) error {
		calls++
		return errors.New("boom")
	})
	if _, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 10); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(fake.visibility) != 0 || len(fake.sent) != 2 {
		t.Errorf("fallback: calls=%d visibility=%d sent=%d, want 2/0/2", calls, len(fake.visibility), len(fake.sent))
	}
}

// cmvFailing fails every ChangeMessageVisibility call while the rest of the
// client keeps working.
type cmvFailing struct{ *fakeSQS }

func (c cmvFailing) ChangeMessageVisibility(context.Context, *awssqs.ChangeMessageVisibilityInput, ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	return nil, errors.New("AWS.SimpleQueueService.MessageNotInflight")
}

func TestAppHandlerFailureReleasesWithZeroDefaultDelay(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))
	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"), babelqueue.WithMaxAttempts(2))
	app.Handle("urn:babel:orders:created", func(context.Context, babelqueue.Envelope) error { return errors.New("boom") })
	if _, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if len(fake.visibility) != 1 || fake.visibility[0].VisibilityTimeout != 0 {
		t.Fatalf("ChangeMessageVisibility = %+v, want one call with 0s", fake.visibility)
	}
	if len(fake.deleted) != 0 || len(fake.sent) != 1 {
		t.Errorf("release must not delete or re-send: deleted=%v sent=%d", fake.deleted, len(fake.sent))
	}
}

func TestAppReleaseErrorLeavesMessageReserved(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(cmvFailing{fake}, WithQueueURLPrefix("http://fake"))
	var releaseErrs []error
	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"), babelqueue.WithMaxAttempts(3),
		babelqueue.WithReleaseErrorHandler(func(_ context.Context, msg *babelqueue.ReceivedMessage, err error) {
			if msg == nil || msg.Handle == "" {
				t.Errorf("release error handler got message %+v", msg)
			}
			releaseErrs = append(releaseErrs, err)
		}))
	app.Handle("urn:babel:orders:created", func(context.Context, babelqueue.Envelope) error { return errors.New("boom") })
	if _, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if len(fake.deleted) != 0 || len(fake.sent) != 1 {
		t.Errorf("a failed CMV must neither delete nor re-send: deleted=%v sent=%d", fake.deleted, len(fake.sent))
	}
	if len(releaseErrs) != 1 || releaseErrs[0] == nil {
		t.Errorf("release errors = %v, want exactly one reported CMV failure", releaseErrs)
	}
}

// Under the unknown-URN release strategy an undecodable body must not be released or retried
// (no handler can ever claim it): with dead-lettering on it is written to the DLQ wrapped
// ({"raw": <body>} plus a poison dead_letter block) in a single delivery, and only then acknowledged.
func TestAppUndecodableBodyUnderReleaseStrategyIsDeadLetteredWrapped(t *testing.T) {
	const body = `not json at all`
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))
	app := babelqueue.NewApp(tr,
		babelqueue.WithDefaultQueue("orders"),
		babelqueue.WithMaxAttempts(3),
		babelqueue.WithDeadLetter(true),
		babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease),
	)
	fake.seed("http://fake/orders", body, 1)

	n, err := app.Drain(context.Background(), "orders", 50)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("processed %d deliveries, want 1 (no retry)", n)
	}
	if len(fake.visibility) != 0 {
		t.Errorf("ChangeMessageVisibility calls = %d, want 0", len(fake.visibility))
	}
	if len(fake.sent) != 1 || aws.ToString(fake.sent[0].QueueUrl) != "http://fake/orders.dlq" {
		t.Fatalf("sends = %d, want exactly one to orders.dlq", len(fake.sent))
	}
	requirePoisonDLQBody(t, aws.ToString(fake.sent[0].MessageBody), body)
	if len(fake.deleted) != 1 {
		t.Errorf("deletes = %v, want exactly the final ack", fake.deleted)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestReconcileAttemptsPreservesUnknownKeys(t *testing.T) {
	body := `{"job":"urn:babel:orders:created","trace_id":"t","data":{},"meta":{"id":"i","queue":"orders",` +
		`"lang":"php","schema_version":1,"created_at":1,"vendor_flag":true},"attempts":0,"extra_top":"x"}`
	out := reconcileAttempts(body, "3")
	env, err := babelqueue.Decode([]byte(out))
	if err != nil {
		t.Fatal(err)
	}
	if env.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", env.Attempts)
	}
	if !strings.Contains(out, `"extra_top":"x"`) || !strings.Contains(out, `"vendor_flag":true`) {
		t.Errorf("unknown keys lost on reconcile: %s", out)
	}
}

// seedWithVersion seeds a message carrying a bq-schema-version String attribute (or none when
// version is nil) and the given ApproximateReceiveCount.
func (f *fakeSQS) seedWithVersion(url, body string, version *string, receiveCount int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	m := types.Message{
		Body:          aws.String(body),
		ReceiptHandle: aws.String("seed-" + strconv.Itoa(f.nextID)),
		Attributes:    map[string]string{"ApproximateReceiveCount": strconv.Itoa(receiveCount)},
	}
	if version != nil {
		m.MessageAttributes = map[string]types.MessageAttributeValue{
			"bq-schema-version": {DataType: aws.String("String"), StringValue: version},
		}
	}
	f.visible[url] = append(f.visible[url], m)
}

func gateBody(t *testing.T) string {
	t.Helper()
	env, err := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestPopSchemaGateDecodesWhenMissingBlankOrSupported(t *testing.T) {
	blank, supported := " \t", "1"
	for name, version := range map[string]*string{"missing": nil, "blank": &blank, "supported": &supported} {
		t.Run(name, func(t *testing.T) {
			fake := newFakeSQS()
			called := false
			tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"),
				WithOnSchemaVersionRejected(func(string, string) { called = true }))
			fake.seedWithVersion("http://fake/orders", gateBody(t), version, 2)

			msg, err := tr.Pop(context.Background(), "orders", 0)
			if err != nil || msg == nil {
				t.Fatalf("Pop: msg=%v err=%v", msg, err)
			}
			if msg.RouteURN != "" {
				t.Errorf("RouteURN = %q, want empty", msg.RouteURN)
			}
			if called {
				t.Error("hook called for an accepted message")
			}
			got, err := babelqueue.Decode([]byte(msg.Body))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if got.Attempts != 1 {
				t.Errorf("attempts = %d, want 1 (reconciled from ApproximateReceiveCount 2)", got.Attempts)
			}
		})
	}
}

func TestPopSchemaGateRejectsUnsupportedWithoutDecoding(t *testing.T) {
	for _, version := range []string{"2", "x", " 1", "\u00a0"} {
		t.Run(version, func(t *testing.T) {
			fake := newFakeSQS()
			var gotVersion, gotQueue string
			tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"),
				WithOnSchemaVersionRejected(func(v, q string) { gotVersion, gotQueue = v, q }))
			// A body that is not JSON proves the gate runs before any decode / reconcile.
			fake.seedWithVersion("http://fake/orders", "not json", &version, 2)

			msg, err := tr.Pop(context.Background(), "orders", 0)
			if err != nil || msg == nil {
				t.Fatalf("Pop: msg=%v err=%v", msg, err)
			}
			if want := unsupportedSchemaRoutePrefix + neturl.QueryEscape(version); msg.RouteURN != want {
				t.Errorf("RouteURN = %q, want %q", msg.RouteURN, want)
			}
			if msg.Body != "not json" {
				t.Errorf("Body = %q, want it handed over untouched", msg.Body)
			}
			if msg.DeliveryCount != 2 {
				t.Errorf("DeliveryCount = %d, want 2", msg.DeliveryCount)
			}
			if gotVersion != version || gotQueue != "orders" {
				t.Errorf("hook got (%q,%q), want (%q,%q)", gotVersion, gotQueue, version, "orders")
			}
		})
	}
}

func TestPopSchemaGateHookPanicIsIgnored(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"),
		WithOnSchemaVersionRejected(func(string, string) { panic("boom") }))
	v := "2"
	fake.seedWithVersion("http://fake/orders", gateBody(t), &v, 2)
	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil || msg == nil || msg.RouteURN == "" {
		t.Fatalf("Pop: msg=%v err=%v", msg, err)
	}
}

// A version-gated message is dead-lettered directly, with no retry: even with WithMaxAttempts(3)
// (and the default Release strategy) the handler is never reached and the first delivery already
// writes the one dead-letter record and deletes the original.
func TestAppQuarantinesVersionGatedMessage(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))
	handled := false
	app := babelqueue.NewApp(tr,
		babelqueue.WithDefaultQueue("orders"),
		babelqueue.WithMaxAttempts(3),
		babelqueue.WithDeadLetter(true),
	)
	app.Handle("urn:babel:orders:created", func(context.Context, babelqueue.Envelope) error {
		handled = true
		return nil
	})
	v := "2"
	incoming := gateBody(t)
	fake.seedWithVersion("http://fake/orders", incoming, &v, 1)

	n, err := app.Drain(context.Background(), "orders", 50)
	if err != nil {
		t.Fatal(err)
	}
	if handled {
		t.Fatal("handler ran for a version-gated message")
	}
	if n != 1 {
		t.Errorf("processed %d deliveries, want 1 (dead-lettered directly, no retry)", n)
	}
	if len(fake.sent) != 1 || aws.ToString(fake.sent[0].QueueUrl) != "http://fake/orders.dlq" {
		t.Fatalf("sends = %d, want exactly one to orders.dlq", len(fake.sent))
	}
	// The DLQ record is the incoming body verbatim (never decoded, no dead_letter block).
	if got := aws.ToString(fake.sent[0].MessageBody); got != incoming {
		t.Errorf("DLQ body = %q, want the incoming body %q byte-for-byte", got, incoming)
	}
	if strings.Contains(aws.ToString(fake.sent[0].MessageBody), "dead_letter") {
		t.Error("DLQ body carries a dead_letter block")
	}
}

// A hostile bq-schema-version is bounded to 64 characters plus "..." in the hook value and the
// route URN; exactly 64 characters is left alone.
func TestPopSchemaGateTruncatesLongDeclaredVersion(t *testing.T) {
	pop := func(version string, opts ...Option) *babelqueue.ReceivedMessage {
		fake := newFakeSQS()
		tr := NewWithClient(fake, append([]Option{WithQueueURLPrefix("http://fake")}, opts...)...)
		fake.seedWithVersion("http://fake/orders", "not json", &version, 1)
		msg, err := tr.Pop(context.Background(), "orders", 0)
		if err != nil || msg == nil {
			t.Fatalf("Pop: msg=%v err=%v", msg, err)
		}
		return msg
	}
	long := strings.Repeat("9", 5000)
	var hooked string
	msg := pop(long, WithOnSchemaVersionRejected(func(v, _ string) { hooked = v }))
	want := strings.Repeat("9", 64) + "..."
	if hooked != want {
		t.Errorf("hook value length = %d, want %d", len(hooked), len(want))
	}
	if route := unsupportedSchemaRoutePrefix + neturl.QueryEscape(want); msg.RouteURN != route {
		t.Errorf("RouteURN length = %d, want %d", len(msg.RouteURN), len(route))
	}
	exact := strings.Repeat("9", 64)
	if msg := pop(exact); msg.RouteURN != unsupportedSchemaRoutePrefix+exact {
		t.Errorf("a 64-character value must not be truncated, got %q", msg.RouteURN)
	}
}
