package sqs

import (
	"context"
	"errors"
	"math"
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

// An undecodable body carries no attempts counter and the in-place release never
// rewrites it, so only ApproximateReceiveCount (ReceivedMessage.DeliveryCount) can
// bound the retries. With the default 0s backoff it must still stop at
// WithMaxAttempts and reach the dead-letter queue instead of looping forever.
func TestAppUndecodableBodyIsBoundedAndDeadLettered(t *testing.T) {
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
		if n != 3 {
			t.Errorf("%q: processed %d deliveries, want 3 (bounded by WithMaxAttempts)", body, n)
		}
		if len(fake.visibility) != 2 {
			t.Errorf("%q: ChangeMessageVisibility calls = %d, want 2", body, len(fake.visibility))
		}
		for _, in := range fake.visibility {
			if in.VisibilityTimeout != 0 {
				t.Errorf("%q: VisibilityTimeout = %d, want 0 (default backoff)", body, in.VisibilityTimeout)
			}
		}
		if len(fake.sent) != 1 || aws.ToString(fake.sent[0].QueueUrl) != "http://fake/orders.dlq" {
			t.Fatalf("%q: sends = %d, want exactly one to orders.dlq", body, len(fake.sent))
		}
		dl, err := babelqueue.Decode([]byte(aws.ToString(fake.sent[0].MessageBody)))
		if err != nil {
			t.Fatalf("%q: dead-letter body: %v", body, err)
		}
		if dl.Attempts != 3 || dl.DeadLetter == nil || dl.DeadLetter.Reason != "unknown_urn" {
			t.Errorf("%q: dead letter attempts=%d dead_letter=%+v, want attempts 3 reason unknown_urn", body, dl.Attempts, dl.DeadLetter)
		}
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

// Under the unknown-URN release strategy an undecodable body must not be released
// forever (no handler can ever claim it): it is bounded by ApproximateReceiveCount and
// dead-lettered, like the fail strategy.
func TestAppUndecodableBodyUnderReleaseStrategyIsBounded(t *testing.T) {
	fake := newFakeSQS()
	tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))
	app := babelqueue.NewApp(tr,
		babelqueue.WithDefaultQueue("orders"),
		babelqueue.WithMaxAttempts(3),
		babelqueue.WithDeadLetter(true),
		babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease),
	)
	fake.seed("http://fake/orders", `not json at all`, 1)

	n, err := app.Drain(context.Background(), "orders", 50)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("processed %d deliveries, want 3 (bounded by WithMaxAttempts)", n)
	}
	if len(fake.visibility) != 2 {
		t.Errorf("ChangeMessageVisibility calls = %d, want 2", len(fake.visibility))
	}
	if len(fake.sent) != 1 || aws.ToString(fake.sent[0].QueueUrl) != "http://fake/orders.dlq" {
		t.Fatalf("sends = %d, want exactly one to orders.dlq", len(fake.sent))
	}
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
