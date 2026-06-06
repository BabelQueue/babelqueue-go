package babelqueue

import (
	"fmt"
	"time"
)

// DeadLetter is the additive block appended to an envelope when a message is
// dead-lettered. The original envelope is preserved unchanged alongside it, so a
// consumer in any language can still read the original job, data and trace_id.
type DeadLetter struct {
	Reason        string  `json:"reason"`
	Error         *string `json:"error"`     // null when no cause was supplied
	Exception     *string `json:"exception"` // null when no cause was supplied
	FailedAt      int64   `json:"failed_at"` // Unix milliseconds, UTC
	OriginalQueue string  `json:"original_queue"`
	Attempts      int     `json:"attempts"`
	Lang          string  `json:"lang"`
}

// Annotate returns a copy of the envelope with a dead_letter block attached,
// recording why and where it failed; it does not mutate the original. A non-nil
// cause fills in the error message and its Go type; both serialize as JSON null
// when cause is nil.
func Annotate(env Envelope, reason, originalQueue string, attempts int, cause error) Envelope {
	dl := &DeadLetter{
		Reason:        reason,
		FailedAt:      time.Now().UnixMilli(),
		OriginalQueue: originalQueue,
		Attempts:      attempts,
		Lang:          SourceLang,
	}
	if cause != nil {
		msg := cause.Error()
		dl.Error = &msg
		typ := fmt.Sprintf("%T", cause)
		dl.Exception = &typ
	}
	env.DeadLetter = dl
	return env
}
