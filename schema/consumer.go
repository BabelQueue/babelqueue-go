package schema

import (
	"context"
	"errors"
	"fmt"
	"strings"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// ErrInvalidPayload is returned (wrapped) when a message's data does not match the schema
// registered for its URN. Detect it with errors.Is.
var ErrInvalidPayload = errors.New("babelqueue/schema: message data does not match its URN schema")

// Check validates a (urn, data) pair against the schema registered for urn. It is the
// producer-side guard: call it before publishing so invalid data never enters the queue.
// It returns nil when the data is valid OR when no schema is registered for the URN
// (opt-in); a provider lookup error is returned wrapped (transient — e.g. the registry file
// is briefly unavailable during a deploy).
func Check(provider Provider, urn string, data map[string]any) error {
	sch, found, err := provider.Schema(urn)
	if err != nil {
		return fmt.Errorf("schema: lookup %q: %w", urn, err)
	}
	if !found {
		return nil
	}
	if violations := sch.Validate(data); len(violations) > 0 {
		return fmt.Errorf("%w for %q: %s", ErrInvalidPayload, urn, strings.Join(violations, "; "))
	}
	return nil
}

// Validate is the envelope form of [Check]: it validates env.Data against env.URN()'s schema.
func Validate(provider Provider, env babelqueue.Envelope) error {
	return Check(provider, env.URN(), env.Data)
}

// Wrap returns handler decorated to validate each message's data against its URN schema
// before the handler runs (consumer-side safety net). It composes with the App's
// ack-on-return / retry-on-error contract:
//
//   - valid data (or no schema registered for the URN) → the handler runs unchanged;
//   - invalid data → [ErrInvalidPayload] is returned, so the message takes the retry /
//     dead-letter path. Because invalid data will not become valid on retry, such a poison
//     message exhausts its attempts and is dead-lettered — prefer [Check] producer-side to
//     keep invalid data out of the queue entirely;
//   - a provider lookup error → returned, so the message is retried once the source recovers.
//
// Register it like any handler: app.Handle(urn, schema.Wrap(provider, handler)).
func Wrap(provider Provider, handler babelqueue.Handler) babelqueue.Handler {
	return func(ctx context.Context, env babelqueue.Envelope) error {
		if err := Validate(provider, env); err != nil {
			return err
		}
		return handler(ctx, env)
	}
}
