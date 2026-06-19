package babelqueue

import (
	"context"
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
		batch = append(batch, pending{msg: msg, env: env, decoded: derr == nil})
	}

	res := RedriveResult{Items: make([]RedriveItem, 0, len(batch))}
	for _, p := range batch {
		if !p.decoded {
			_ = t.Publish(ctx, dlq, p.msg.Body) // restore the poison body; never drop it
			_ = t.Ack(ctx, p.msg)
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
			_ = t.Publish(ctx, dlq, p.msg.Body) // not selected: restore unchanged
			_ = t.Ack(ctx, p.msg)
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
			_ = t.Publish(ctx, dlq, p.msg.Body) // report the plan; restore unchanged
			_ = t.Ack(ctx, p.msg)
			res.Skipped++
			res.Items = append(res.Items, item)
			continue
		}

		reset := p.env
		reset.DeadLetter = nil
		reset.Attempts = 0
		body, err := reset.Encode()
		if err != nil {
			_ = t.Publish(ctx, dlq, p.msg.Body) // restore on a re-encode failure
			_ = t.Ack(ctx, p.msg)
			return res, err
		}
		if err := t.Publish(ctx, target, string(body)); err != nil {
			_ = t.Publish(ctx, dlq, p.msg.Body) // restore on a publish failure
			_ = t.Ack(ctx, p.msg)
			return res, err
		}
		_ = t.Ack(ctx, p.msg)
		item.Redriven = true
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
