package outbox

import (
	"context"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// defaultDrainCeiling is the hard safety ceiling on [Relay.Drain] passes when the caller
// passes a non-positive maxPasses.
const defaultDrainCeiling = 10000

// Result summarizes a relay pass: how many pending rows were published and how many
// failed (and were left pending for a later retry).
type Result struct {
	Published int
	Failed    int
}

// Attempted is the total rows the relay touched (published + failed).
func (r Result) Attempted() int { return r.Published + r.Failed }

// Relay is the read/publish side of the transactional outbox (ADR-0029): it drains the
// pending rows an [Outbox] committed and forwards each onto the broker through the frozen
// babelqueue [babelqueue.Transport] seam — the same publish-only contract every
// framework-less producer uses — marking every row published or failed.
//
// Run it on a short interval (a worker loop, a scheduled command) after the business
// transaction commits. It only ever reads already-durable rows, so it never invents work.
//
// Semantics — at-least-once handoff:
//
//   - A row is marked published only AFTER [babelqueue.Transport.Publish] returns; if the
//     process dies between publish and [Store.MarkPublished], the row stays pending and is
//     published again on the next pass. That is at-least-once: a downstream consumer must
//     dedupe on the canonical meta.id (github.com/babelqueue/babelqueue-go/idempotency is
//     that guard, the consumer-side mirror of this producer-side helper — ADR-0022).
//   - A publish that errors is caught, [Store.MarkFailed] records the reason and bumps the
//     attempt count, and the row stays pending for a later retry. One poison row never
//     blocks the rest of the batch.
//   - trace_id is preserved end-to-end (GR-4): the relay publishes the stored bytes
//     verbatim — it never decodes, rebuilds or re-encodes the envelope — so the body that
//     reaches the broker is byte-identical to what was stored (GR-1/GR-5).
//
// Backoff: between a failed publish and continuing within the same pass, the relay sleeps
// for a bounded, linearly-growing delay (capped), to avoid hammering a broker that is
// briefly down. The sleeper is injectable so tests stay instant.
type Relay struct {
	transport   babelqueue.Transport
	store       Store
	batchSize   int
	backoffStep time.Duration
	backoffCap  time.Duration
	sleep       func(time.Duration)
}

// Options configures a [Relay]. The zero value selects sensible defaults: a batch of 100,
// a 50ms backoff step, a 5s backoff cap, and time.Sleep as the sleeper.
type Options struct {
	// BatchSize is how many rows to reserve and publish per [Relay.Flush] (default 100).
	BatchSize int
	// BackoffStep is the base backoff added per prior attempt (default 50ms).
	BackoffStep time.Duration
	// BackoffCap is the upper bound on a single backoff sleep (default 5s).
	BackoffCap time.Duration
	// Sleeper sleeps for d; defaults to time.Sleep. Inject a no-op (or recorder) in tests
	// so they stay instant.
	Sleeper func(d time.Duration)
}

// NewRelay returns a relay that drains store and publishes through transport, configured
// by opts (the zero Options selects defaults).
func NewRelay(transport babelqueue.Transport, store Store, opts Options) *Relay {
	r := &Relay{
		transport:   transport,
		store:       store,
		batchSize:   opts.BatchSize,
		backoffStep: opts.BackoffStep,
		backoffCap:  opts.BackoffCap,
		sleep:       opts.Sleeper,
	}
	if r.batchSize <= 0 {
		r.batchSize = 100
	}
	if r.backoffStep <= 0 {
		r.backoffStep = 50 * time.Millisecond
	}
	if r.backoffCap <= 0 {
		r.backoffCap = 5 * time.Second
	}
	if r.sleep == nil {
		r.sleep = time.Sleep
	}
	return r
}

// Flush publishes one batch of pending rows. Each row the transport accepts is marked
// published; each that errors is marked failed (with a backoff before continuing) and
// left pending. It returns a per-pass tally. Call it repeatedly (a loop / cron) to drain
// the outbox; [Relay.Drain] loops until the outbox is empty.
//
// The stored body rides verbatim: it is handed straight to
// [babelqueue.Transport.Publish], never decoded or re-encoded.
func (r *Relay) Flush(ctx context.Context) (Result, error) {
	records, err := r.store.FetchUnpublished(r.batchSize)
	if err != nil {
		return Result{}, err
	}

	var publishedIDs []string
	failed := 0

	for _, rec := range records {
		if perr := r.transport.Publish(ctx, rec.Queue, rec.Body); perr != nil {
			if merr := r.store.MarkFailed(rec.ID, perr.Error()); merr != nil {
				return Result{}, merr
			}
			failed++
			r.sleep(r.backoffFor(rec.Attempts))
			continue
		}
		publishedIDs = append(publishedIDs, rec.ID)
	}

	if len(publishedIDs) > 0 {
		if merr := r.store.MarkPublished(publishedIDs); merr != nil {
			return Result{}, merr
		}
	}

	return Result{Published: len(publishedIDs), Failed: failed}, nil
}

// Drain empties the outbox by repeatedly calling [Relay.Flush] while each pass keeps
// making progress (publishes at least one row), then returns the cumulative tally. The
// loop stops as soon as a pass publishes nothing — the outbox is empty, or only currently
// failing rows remain (those are left pending for a future Drain once the broker
// recovers). maxPasses is a hard safety ceiling so a degenerate store can never spin
// forever (a non-positive value uses a generous internal default).
func (r *Relay) Drain(ctx context.Context, maxPasses int) (Result, error) {
	ceiling := maxPasses
	if ceiling <= 0 {
		ceiling = defaultDrainCeiling
	}

	var total Result
	for pass := 0; pass < ceiling; pass++ {
		res, err := r.Flush(ctx)
		if err != nil {
			return total, err
		}
		total.Published += res.Published
		total.Failed += res.Failed

		// No progress this pass → drained, or only failing rows remain. Stop.
		if res.Published == 0 {
			break
		}
	}

	return total, nil
}

// backoffFor is the backoff for a row that has already failed priorAttempts times: a
// linear step per attempt, capped. Kept simple and deterministic so the budget is obvious.
func (r *Relay) backoffFor(priorAttempts int) time.Duration {
	steps := priorAttempts + 1
	if steps < 1 {
		steps = 1
	}
	delay := r.backoffStep * time.Duration(steps)
	if delay > r.backoffCap {
		return r.backoffCap
	}
	return delay
}
