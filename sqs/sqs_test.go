package sqs

import (
	"context"
	"errors"
	"strconv"
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
	err         error // when set, every call returns it
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
	return &awssqs.ReceiveMessageOutput{Messages: []types.Message{m}}, nil
}

func (f *fakeSQS) DeleteMessage(_ context.Context, in *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	handle := aws.ToString(in.ReceiptHandle)
	f.deleted = append(f.deleted, handle)
	delete(f.inflight, handle)
	return &awssqs.DeleteMessageOutput{}, nil
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
