package machinery

import (
	"context"
	"errors"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"

	"github.com/RichardKnop/machinery/v2/backends/result"
	"github.com/RichardKnop/machinery/v2/tasks"
)

// fakeSender captures the signature instead of touching a broker.
type fakeSender struct {
	sent *tasks.Signature
	err  error
}

func (f *fakeSender) SendTaskWithContext(_ context.Context, sig *tasks.Signature) (*result.AsyncResult, error) {
	f.sent = sig
	return nil, f.err
}

// fakeRegistrar captures the registered task name + func.
type fakeRegistrar struct {
	name string
	fn   interface{}
	err  error
}

func (f *fakeRegistrar) RegisterTask(name string, fn interface{}) error {
	f.name, f.fn = name, fn
	return f.err
}

func mustEnv(t *testing.T) babelqueue.Envelope {
	t.Helper()
	env, err := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": 1042}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatalf("Make: %v", err)
	}
	return env
}

func TestSignatureNameIsURNAndArgIsTheEnvelope(t *testing.T) {
	env := mustEnv(t)
	sig, err := Signature(env)
	if err != nil {
		t.Fatalf("Signature: %v", err)
	}
	if sig.Name != env.URN() {
		t.Errorf("Name = %q, want URN %q", sig.Name, env.URN())
	}
	if len(sig.Args) != 1 || sig.Args[0].Type != "string" {
		t.Fatalf("want a single string arg, got %+v", sig.Args)
	}
	body, _ := env.Encode()
	if sig.Args[0].Value.(string) != string(body) {
		t.Errorf("arg is not the byte-identical envelope\n got: %s\nwant: %s", sig.Args[0].Value, body)
	}
}

func TestSignatureEmptyURN(t *testing.T) {
	if _, err := Signature(babelqueue.Envelope{}); !errors.Is(err, babelqueue.ErrEmptyURN) {
		t.Errorf("err = %v, want ErrEmptyURN", err)
	}
}

func TestSignatureForBuildsTheEnvelope(t *testing.T) {
	sig, err := SignatureFor("urn:babel:orders:created", map[string]any{"order_id": 7})
	if err != nil {
		t.Fatalf("SignatureFor: %v", err)
	}
	env, err := Envelope(sig)
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	if env.URN() != "urn:babel:orders:created" || env.Data["order_id"].(float64) != 7 {
		t.Errorf("round-trip mismatch: %+v", env)
	}
}

func TestEnvelopeRejectsMissingOrBadArg(t *testing.T) {
	if _, err := Envelope(&tasks.Signature{Name: "x"}); !errors.Is(err, ErrNoEnvelopeArg) {
		t.Errorf("no args: err = %v, want ErrNoEnvelopeArg", err)
	}
	if _, err := Envelope(&tasks.Signature{Args: []tasks.Arg{{Type: "int", Value: 5}}}); !errors.Is(err, ErrNoEnvelopeArg) {
		t.Errorf("non-string arg: err = %v, want ErrNoEnvelopeArg", err)
	}
}

func TestProducerDispatchAndSend(t *testing.T) {
	s := &fakeSender{}
	p := NewProducer(s)

	if _, err := p.Dispatch(context.Background(), "urn:babel:orders:created", map[string]any{"order_id": 1}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if s.sent == nil || s.sent.Name != "urn:babel:orders:created" {
		t.Errorf("Dispatch did not send a URN-named signature: %+v", s.sent)
	}

	if _, err := p.Send(context.Background(), mustEnv(t)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if s.sent.Name != "urn:babel:orders:created" {
		t.Errorf("Send name = %q", s.sent.Name)
	}
}

func TestProducerDispatchEmptyURN(t *testing.T) {
	if _, err := NewProducer(&fakeSender{}).Dispatch(context.Background(), "", nil); !errors.Is(err, babelqueue.ErrEmptyURN) {
		t.Errorf("err = %v, want ErrEmptyURN", err)
	}
}

func TestBindDecodesValidatesAndDispatches(t *testing.T) {
	var got babelqueue.Envelope
	fn := Bind(func(_ context.Context, env babelqueue.Envelope) error {
		got = env
		return nil
	})

	body, _ := mustEnv(t).Encode()
	if err := fn(context.Background(), string(body)); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if got.URN() != "urn:babel:orders:created" {
		t.Errorf("handler got %q", got.URN())
	}
}

func TestBindRejectsUnacceptableAndBadJSON(t *testing.T) {
	called := false
	fn := Bind(func(context.Context, babelqueue.Envelope) error { called = true; return nil })

	// Unacceptable: a syntactically valid envelope with an empty URN.
	if err := fn(context.Background(), `{"job":"","trace_id":"t","data":{},"meta":{"schema_version":1},"attempts":0}`); !errors.Is(err, ErrNotAccepted) {
		t.Errorf("unacceptable: err = %v, want ErrNotAccepted", err)
	}
	// Malformed JSON → decode error.
	if err := fn(context.Background(), "not json"); err == nil {
		t.Error("bad json: want a decode error")
	}
	if called {
		t.Error("handler must not run for rejected payloads")
	}
}

func TestRegisterWiresHandlerUnderTheURN(t *testing.T) {
	r := &fakeRegistrar{}
	if err := Register(r, "urn:babel:orders:created", func(context.Context, babelqueue.Envelope) error { return nil }); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if r.name != "urn:babel:orders:created" {
		t.Errorf("registered name = %q", r.name)
	}
	if _, ok := r.fn.(func(context.Context, string) error); !ok {
		t.Errorf("registered func has the wrong machinery signature: %T", r.fn)
	}
}
