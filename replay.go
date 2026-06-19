package babelqueue

import "context"

// HeaderReplayBypass is the out-of-band transport header [Redrive] stamps (when its options
// set Bypass) on a replayed message, and that the [App] runtime surfaces to the handler as
// [IsReplay]. It lets a handler skip external side-effects that already ran, with no change to
// the frozen envelope (ADR-0027).
const HeaderReplayBypass = "bq-replay-bypass"

type replayKey struct{}

func withReplay(ctx context.Context) context.Context {
	return context.WithValue(ctx, replayKey{}, true)
}

// IsReplay reports whether the message currently being handled was redriven with the
// replay-bypass marker set — i.e. this is a deliberate replay, and external side-effects that
// already happened should be skipped. It reads the marker the runtime put on the context from
// the [HeaderReplayBypass] transport header.
func IsReplay(ctx context.Context) bool {
	v, _ := ctx.Value(replayKey{}).(bool)
	return v
}

// BypassExternalEffects runs fn unless the current message is a replay (see [IsReplay]), in
// which case fn is skipped and nil is returned. Wrap the external, non-idempotent side of a
// handler — sending an email, charging a card, calling a third party — so a replay re-runs the
// idempotent core but does not re-fire effects that already happened:
//
//	app.Handle("urn:babel:orders:created", func(ctx context.Context, env babelqueue.Envelope) error {
//		saveOrder(env)                       // idempotent core — always runs
//		return babelqueue.BypassExternalEffects(ctx, func() error {
//			return sendConfirmationEmail(env) // external effect — skipped on replay
//		})
//	})
func BypassExternalEffects(ctx context.Context, fn func() error) error {
	if IsReplay(ctx) {
		return nil
	}
	return fn()
}
