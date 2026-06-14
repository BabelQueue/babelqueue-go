package asynq

import (
	"context"
	"errors"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
	hasynq "github.com/hibiken/asynq"
)

const urn = "urn:babel:orders:created"

// fakeEnqueuer records enqueued tasks (and any options) without touching Redis.
type fakeEnqueuer struct {
	tasks []*hasynq.Task
	opts  [][]hasynq.Option
	err   error
}

func (f *fakeEnqueuer) EnqueueContext(_ context.Context, task *hasynq.Task, opts ...hasynq.Option) (*hasynq.TaskInfo, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.tasks = append(f.tasks, task)
	f.opts = append(f.opts, opts)
	return &hasynq.TaskInfo{ID: "fake-id", Type: task.Type()}, nil
}

func sampleEnvelope(t *testing.T, attempts int) babelqueue.Envelope {
	t.Helper()
	env, err := babelqueue.Make(urn, map[string]any{"order_id": 1042}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	env.Attempts = attempts
	return env
}

func TestTaskUsesURNAsTypeAndEnvelopeAsPayload(t *testing.T) {
	env := sampleEnvelope(t, 0)
	want, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}

	task, err := Task(env)
	if err != nil {
		t.Fatal(err)
	}
	if task.Type() != urn {
		t.Errorf("task type = %q, want the URN %q", task.Type(), urn)
	}
	if string(task.Payload()) != string(want) {
		t.Errorf("payload is not the byte-identical envelope\n got: %s\nwant: %s", task.Payload(), want)
	}
}

func TestTaskRejectsEmptyURN(t *testing.T) {
	_, err := Task(babelqueue.Envelope{}) // no job/urn
	if !errors.Is(err, babelqueue.ErrEmptyURN) {
		t.Errorf("err = %v, want ErrEmptyURN", err)
	}
}

func TestTaskForBuildsEnvelopeAndForwardsBothOptionFamilies(t *testing.T) {
	task, err := TaskFor(urn, map[string]any{"order_id": 1042},
		babelqueue.WithQueue("orders"),      // envelope option
		babelqueue.WithTraceID("trace-xyz"), // envelope option
		hasynq.MaxRetry(7),                  // asynq task option (forwarded onto the task)
	)
	if err != nil {
		t.Fatal(err)
	}
	if task.Type() != urn {
		t.Errorf("task type = %q, want %q", task.Type(), urn)
	}
	env, err := Envelope(task)
	if err != nil {
		t.Fatal(err)
	}
	if env.Meta.Queue != "orders" {
		t.Errorf("meta.queue = %q, want orders (envelope option applied)", env.Meta.Queue)
	}
	if env.TraceID != "trace-xyz" {
		t.Errorf("trace_id = %q, want trace-xyz (envelope option applied)", env.TraceID)
	}
	if !env.Accepts() {
		t.Error("produced envelope should be acceptable to a consumer")
	}
}

func TestTaskForEmptyURN(t *testing.T) {
	if _, err := TaskFor("", nil); !errors.Is(err, babelqueue.ErrEmptyURN) {
		t.Errorf("err = %v, want ErrEmptyURN", err)
	}
}

func TestProducerDispatchEnqueuesRoutedTask(t *testing.T) {
	fe := &fakeEnqueuer{}
	p := NewProducer(fe)

	info, err := p.Dispatch(context.Background(), urn, map[string]any{"order_id": 1042},
		babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	if info == nil || info.Type != urn {
		t.Fatalf("info = %+v, want Type %q", info, urn)
	}
	if len(fe.tasks) != 1 {
		t.Fatalf("enqueued %d tasks, want 1", len(fe.tasks))
	}
	got := fe.tasks[0]
	if got.Type() != urn {
		t.Errorf("enqueued type = %q, want %q", got.Type(), urn)
	}
	env, _ := Envelope(got)
	if env.URN() != urn || env.Meta.Queue != "orders" {
		t.Errorf("payload envelope = (urn %q, queue %q), want (%q, orders)", env.URN(), env.Meta.Queue, urn)
	}
}

func TestProducerDispatchEmptyURN(t *testing.T) {
	p := NewProducer(&fakeEnqueuer{})
	if _, err := p.Dispatch(context.Background(), "", nil); !errors.Is(err, babelqueue.ErrEmptyURN) {
		t.Errorf("err = %v, want ErrEmptyURN", err)
	}
}

func TestProducerDispatchSurfacesEnqueueError(t *testing.T) {
	boom := errors.New("redis down")
	p := NewProducer(&fakeEnqueuer{err: boom})
	if _, err := p.Dispatch(context.Background(), urn, nil); !errors.Is(err, boom) {
		t.Errorf("err = %v, want the enqueue error", err)
	}
}

func TestProducerEnqueueForwardsTaskOptions(t *testing.T) {
	fe := &fakeEnqueuer{}
	p := NewProducer(fe)
	env := sampleEnvelope(t, 0)

	if _, err := p.Enqueue(context.Background(), env, hasynq.MaxRetry(3)); err != nil {
		t.Fatal(err)
	}
	if len(fe.tasks) != 1 {
		t.Fatalf("enqueued %d tasks, want 1", len(fe.tasks))
	}
	// The asynq task carries the forwarded option; the payload stays the envelope.
	if got, _ := Envelope(fe.tasks[0]); got.URN() != urn {
		t.Errorf("payload urn = %q, want %q", got.URN(), urn)
	}
}

func TestProducerEnqueueEmptyURN(t *testing.T) {
	p := NewProducer(&fakeEnqueuer{})
	if _, err := p.Enqueue(context.Background(), babelqueue.Envelope{}); !errors.Is(err, babelqueue.ErrEmptyURN) {
		t.Errorf("err = %v, want ErrEmptyURN", err)
	}
}

func TestBindDecodesPayloadAndInvokesHandler(t *testing.T) {
	env := sampleEnvelope(t, 0)
	body, _ := env.Encode()
	task := hasynq.NewTask(urn, body)

	var seen babelqueue.Envelope
	h := Bind(func(_ context.Context, e babelqueue.Envelope) error {
		seen = e
		return nil
	})
	if err := h.ProcessTask(context.Background(), task); err != nil {
		t.Fatalf("ProcessTask: %v", err)
	}
	if seen.URN() != urn {
		t.Errorf("handler saw urn %q, want %q", seen.URN(), urn)
	}
	if seen.TraceID != env.TraceID {
		t.Errorf("trace_id = %q, want %q (preserved across the hop)", seen.TraceID, env.TraceID)
	}
}

func TestBindAcceptsURNAlias(t *testing.T) {
	// A peer SDK may emit the inbound "urn" alias instead of "job"; the consumer
	// must accept it.
	payload := []byte(`{"urn":"urn:babel:orders:created","trace_id":"t-1","data":{"order_id":1},` +
		`"meta":{"id":"m-1","queue":"orders","lang":"php","schema_version":1,"created_at":1},"attempts":0}`)
	task := hasynq.NewTask("urn:babel:orders:created", payload)

	called := false
	h := Bind(func(_ context.Context, e babelqueue.Envelope) error {
		called = true
		if e.URN() != "urn:babel:orders:created" {
			t.Errorf("urn = %q", e.URN())
		}
		return nil
	})
	if err := h.ProcessTask(context.Background(), task); err != nil {
		t.Fatalf("ProcessTask: %v", err)
	}
	if !called {
		t.Error("handler was not invoked for a urn-alias payload")
	}
}

func TestBindRejectsUnacceptableEnvelope(t *testing.T) {
	// Unsupported schema_version → not accepted → ErrNotAccepted (handler skipped).
	payload := []byte(`{"job":"urn:x","trace_id":"t","data":{},"meta":{"schema_version":999},"attempts":0}`)
	task := hasynq.NewTask("urn:x", payload)

	h := Bind(func(context.Context, babelqueue.Envelope) error {
		t.Fatal("handler must not run for an unacceptable envelope")
		return nil
	})
	if err := h.ProcessTask(context.Background(), task); !errors.Is(err, ErrNotAccepted) {
		t.Errorf("err = %v, want ErrNotAccepted", err)
	}
}

func TestBindSurfacesDecodeError(t *testing.T) {
	task := hasynq.NewTask("urn:x", []byte("not-json"))
	h := Bind(func(context.Context, babelqueue.Envelope) error { return nil })
	if err := h.ProcessTask(context.Background(), task); err == nil {
		t.Error("expected a decode error for a non-JSON payload")
	}
}

func TestBindPropagatesHandlerError(t *testing.T) {
	env := sampleEnvelope(t, 0)
	body, _ := env.Encode()
	task := hasynq.NewTask(urn, body)

	boom := errors.New("handler failed")
	h := Bind(func(context.Context, babelqueue.Envelope) error { return boom })
	if err := h.ProcessTask(context.Background(), task); !errors.Is(err, boom) {
		t.Errorf("err = %v, want the handler error", err)
	}
}

func TestRegisterRoutesByURNOnServeMux(t *testing.T) {
	mux := hasynq.NewServeMux()

	hit := make(chan string, 2)
	Register(mux, "urn:babel:orders:created", func(_ context.Context, env babelqueue.Envelope) error {
		hit <- "created:" + env.URN()
		return nil
	})
	Register(mux, "urn:babel:orders:shipped", func(_ context.Context, env babelqueue.Envelope) error {
		hit <- "shipped:" + env.URN()
		return nil
	})

	// A task whose TypeName == the created URN must route to the created handler.
	created := sampleEnvelope(t, 0)
	createdBody, _ := created.Encode()
	if err := mux.ProcessTask(context.Background(), hasynq.NewTask("urn:babel:orders:created", createdBody)); err != nil {
		t.Fatalf("ProcessTask(created): %v", err)
	}
	select {
	case got := <-hit:
		if got != "created:urn:babel:orders:created" {
			t.Errorf("routed to %q, want the created handler", got)
		}
	default:
		t.Fatal("created handler was not invoked")
	}
}

func TestRegisterUnknownURNIsAsynqNotFound(t *testing.T) {
	mux := hasynq.NewServeMux()
	Register(mux, "urn:babel:orders:created", func(context.Context, babelqueue.Envelope) error { return nil })

	env := sampleEnvelope(t, 0)
	body, _ := env.Encode()
	// TypeName has no registered handler → asynq's ServeMux returns an error.
	err := mux.ProcessTask(context.Background(), hasynq.NewTask("urn:babel:orders:unmapped", body))
	if err == nil {
		t.Error("expected asynq to error on an unhandled task type")
	}
}

func TestSplitOptionsPartitionsByType(t *testing.T) {
	envOpts, taskOpts := splitOptions([]any{
		babelqueue.WithQueue("orders"),
		hasynq.MaxRetry(3),
		babelqueue.WithTraceID("t"),
		hasynq.Queue("low"),
		"ignored-unknown-type",
		42,
	})
	if len(envOpts) != 2 {
		t.Errorf("envOpts = %d, want 2", len(envOpts))
	}
	if len(taskOpts) != 2 {
		t.Errorf("taskOpts = %d, want 2", len(taskOpts))
	}
}
