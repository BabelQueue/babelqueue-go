package babelqueue

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// RedriveOptions configures a [Redrive] run.
type RedriveOptions struct {
	// ToQueue overrides where messages are re-published. When empty, each message goes back
	// to its own dead_letter.original_queue; set it to a sandbox queue to replay safely.
	ToQueue string
	// Max caps how many messages are pulled from the DLQ (0 = all currently available).
	Max int
	// DryRun inspects without redriving: every message is read, reported, and returned to the
	// DLQ unchanged — nothing is re-published to a source/sandbox queue.
	DryRun bool
	// Select, when non-nil, picks which messages to redrive (e.g. by reason or URN).
	// Unselected messages are returned to the DLQ unchanged.
	Select func(Envelope) bool
	// Bypass stamps the bq-replay-bypass transport header on each redriven message, so a
	// handler can skip external side-effects that already ran (see [BypassExternalEffects]).
	// It takes effect only when the transport is a [HeaderPublisher]; otherwise it is a no-op
	// and the per-item Bypassed flag stays false (ADR-0027).
	Bypass bool
	// Timeout is the per-pop wait passed to the transport (default 1s).
	Timeout time.Duration
}

// RedriveItem records what happened to one message during a [Redrive] run.
type RedriveItem struct {
	MessageID string // meta.id ("" if absent or undecodable)
	TraceID   string
	URN       string
	Reason    string // dead_letter.reason
	From      string // the DLQ it was read from
	To        string // target queue (the plan, even on a dry run; "" when skipped/undecodable)
	Redriven  bool   // true only when actually re-published to To
	Bypassed  bool   // true when the bq-replay-bypass header was stamped on the redriven message
}

// RedriveResult summarizes a [Redrive] run.
type RedriveResult struct {
	Redriven int
	Skipped  int
	Items    []RedriveItem
}

// Redrive moves dead-lettered messages off the dlq queue and re-publishes each to its source
// queue (its dead_letter.original_queue) or to opts.ToQueue, resetting it for reprocessing:
// the dead_letter block is removed and attempts reset to 0, while job, trace_id, data and meta
// are preserved verbatim. It is the operator-side counterpart to the runtime's dead-letter
// routing — the contract leaves redrive to tooling, and this is that tool (ADR-0026).
//
// Messages are drained from the DLQ first and then processed, so restored messages (skipped,
// dry-run, or undecodable) are never re-encountered in the same run. A DLQ message is
// acknowledged only after its re-publish succeeds; an undecodable body is restored, not lost.
func Redrive(ctx context.Context, t Transport, dlq string, opts RedriveOptions) (RedriveResult, error) {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}

	type pending struct {
		msg     *ReceivedMessage
		env     Envelope
		decoded bool
	}
	var batch []pending
	for opts.Max == 0 || len(batch) < opts.Max {
		msg, err := t.Pop(ctx, dlq, timeout)
		if err != nil {
			return RedriveResult{}, err
		}
		if msg == nil {
			break
		}
		env, derr := Decode([]byte(msg.Body))
		if derr == nil {
			// Non-fatal decode findings (a dropped forbidden key, ADR-0031 (c)) are logged,
			// never fatal: the message is still redriven.
			for _, w := range env.Warnings() {
				slog.WarnContext(ctx, "babelqueue: decode warning", "warning", w, "queue", dlq)
			}
		}
		batch = append(batch, pending{msg: msg, env: env, decoded: derr == nil})
	}

	res := RedriveResult{Items: make([]RedriveItem, 0, len(batch))}
	for _, p := range batch {
		if !p.decoded {
			if rerr := restoreToDLQ(ctx, t, dlq, p.msg); rerr != nil { // restore the poison body; never drop it
				return res, rerr
			}
			res.Skipped++
			res.Items = append(res.Items, RedriveItem{From: dlq})
			continue
		}

		item := RedriveItem{
			MessageID: p.env.Meta.ID,
			TraceID:   p.env.TraceID,
			URN:       p.env.URN(),
			From:      dlq,
		}
		if p.env.DeadLetter != nil {
			item.Reason = p.env.DeadLetter.Reason
		}

		if opts.Select != nil && !opts.Select(p.env) {
			if rerr := restoreToDLQ(ctx, t, dlq, p.msg); rerr != nil { // not selected: restore unchanged
				return res, rerr
			}
			res.Skipped++
			res.Items = append(res.Items, item)
			continue
		}

		target := opts.ToQueue
		if target == "" {
			target = sourceQueueOf(p.env)
		}
		item.To = target

		if opts.DryRun {
			if rerr := restoreToDLQ(ctx, t, dlq, p.msg); rerr != nil { // report the plan; restore unchanged
				return res, rerr
			}
			res.Skipped++
			res.Items = append(res.Items, item)
			continue
		}

		reset := p.env
		reset.DeadLetter = nil
		reset.Attempts = 0
		body, err := reset.Encode()
		if err != nil {
			// restore on a re-encode failure; no ack (and a rewind) if that fails
			return res, abortRedrive(ctx, t, dlq, p.msg, err)
		}
		bypassed, err := publishRedriven(ctx, t, target, string(body), opts.Bypass, p.msg)
		if err != nil {
			// restore on a publish failure; no ack (and a rewind) if that fails
			return res, abortRedrive(ctx, t, dlq, p.msg, err)
		}
		_ = t.Ack(ctx, p.msg)
		item.Redriven = true
		item.Bypassed = bypassed
		res.Redriven++
		res.Items = append(res.Items, item)
	}
	return res, nil
}

// sourceQueueOf returns where a redriven message should go by default: its
// dead_letter.original_queue, falling back to meta.queue.
func sourceQueueOf(env Envelope) string {
	if env.DeadLetter != nil && env.DeadLetter.OriginalQueue != "" {
		return env.DeadLetter.OriginalQueue
	}
	return env.Meta.Queue
}

// publishRedriven re-publishes a reset message to queue on behalf of the DLQ message from. When
// bypass is set and the transport is a [HeaderPublisher], it stamps the bq-replay-bypass header
// and reports bypassed=true (that path carries no record key); otherwise it re-publishes through
// [republishFrom] and reports bypassed=false.
func publishRedriven(ctx context.Context, t Transport, queue, body string, bypass bool, from *ReceivedMessage) (bool, error) {
	if bypass {
		if hp, ok := t.(HeaderPublisher); ok {
			return true, hp.PublishWithHeaders(ctx, queue, body, map[string]string{HeaderReplayBypass: "1"})
		}
	}
	return false, republishFrom(ctx, t, queue, body, from)
}

// restoreToDLQ writes the reserved DLQ message back to the DLQ unchanged and acknowledges it only
// when that write succeeded. A failed restore leaves the message unacknowledged and returns the
// write error: the original is never dropped. When the transport implements [Rewinder] the
// message is also handed back to the broker, because on an offset-committing broker (Kafka) a
// later commit is cumulative and would otherwise move the group past the unacknowledged record
// and lose it. If the rewind fails too, both errors are returned together.
func restoreToDLQ(ctx context.Context, t Transport, dlq string, msg *ReceivedMessage) error {
	if err := republishFrom(ctx, t, dlq, msg.Body, msg); err != nil {
		if rw, ok := t.(Rewinder); ok {
			if rerr := rw.Rewind(ctx, msg); rerr != nil {
				return errors.Join(err, rerr)
			}
		}
		return err
	}
	_ = t.Ack(ctx, msg)
	return nil
}

// abortRedrive ends a run on cause (a re-encode or publish failure for msg). msg is restored to
// the DLQ unchanged. The run stops here, so the rest of the drained batch is never processed or
// acknowledged: when the transport implements [Rewinder] it is handed back to the broker too,
// otherwise a later cumulative commit by the same consumer could skip those records. cause is
// returned as-is when the restore and the rewind both succeed; any restore or rewind error is
// joined to it.
func abortRedrive(ctx context.Context, t Transport, dlq string, msg *ReceivedMessage, cause error) error {
	if rerr := restoreToDLQ(ctx, t, dlq, msg); rerr != nil {
		return errors.Join(cause, rerr) // restoreToDLQ already rewound
	}
	if rw, ok := t.(Rewinder); ok {
		if rerr := rw.Rewind(ctx, msg); rerr != nil {
			return errors.Join(cause, rerr)
		}
	}
	return cause
}

// republishFrom re-publishes body to queue on behalf of the reserved message from. A transport
// implementing [KeyedRepublisher] (Kafka: the record key, contract §6.5) takes over the whole
// re-publish so the DLQ record's key is carried byte-for-byte; otherwise a non-empty
// from.RouteURN is carried back through [RoutedPublisher] when available, else a plain Publish.
// It is used for both the redrive write and the restore write back to the DLQ.
func republishFrom(ctx context.Context, t Transport, queue, body string, from *ReceivedMessage) error {
	if kr, ok := t.(KeyedRepublisher); ok {
		return kr.PublishFrom(ctx, queue, body, from)
	}
	return publishKeepingRoute(ctx, t, queue, body, from.RouteURN)
}
