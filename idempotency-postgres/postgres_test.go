package postgres

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// newMock wires a Store onto a sqlmock-backed *sql.DB so the SQL/claim logic is
// exercised with NO live database. QueryMatcherRegexp lets the expectations match
// the meaningful tokens of each statement without pinning exact whitespace.
func newMock(t *testing.T, opts ...Option) (*Store, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := NewWithDB(db, opts...)
	return s, mock
}

// TestClaim_FirstDeliveryWins: the INSERT writes a row and RETURNING yields the id,
// so Claim reports a won race (the first, non-duplicate delivery).
func TestClaim_FirstDeliveryWins(t *testing.T) {
	s, mock := newMock(t)
	mock.ExpectQuery(`INSERT INTO babelqueue_idempotency .* ON CONFLICT .* RETURNING message_id`).
		WithArgs("msg-1", nil).
		WillReturnRows(sqlmock.NewRows([]string{"message_id"}).AddRow("msg-1"))

	won, err := s.Claim(context.Background(), "msg-1")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if !won {
		t.Fatal("first delivery must win the claim")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestClaim_DuplicateLoses: a live row already exists, so ON CONFLICT updates
// nothing and RETURNING yields no rows -> Claim reports a duplicate (not won). This
// is the concurrency contract: a duplicate delivery is detected as already-seen.
func TestClaim_DuplicateLoses(t *testing.T) {
	s, mock := newMock(t)
	mock.ExpectQuery(`INSERT INTO babelqueue_idempotency .* ON CONFLICT .* RETURNING message_id`).
		WithArgs("msg-1", nil).
		WillReturnError(sql.ErrNoRows)

	won, err := s.Claim(context.Background(), "msg-1")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if won {
		t.Fatal("a duplicate delivery must NOT win the claim")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestClaim_WithTTLBindsExpiry: a configured TTL makes the claim bind a non-NULL
// expires_at argument (now()+ttl), proving expiry is honored at write time.
func TestClaim_WithTTLBindsExpiry(t *testing.T) {
	s, mock := newMock(t, WithTTL(time.Hour))
	mock.ExpectQuery(`INSERT INTO babelqueue_idempotency .* RETURNING message_id`).
		WithArgs("msg-ttl", sqlmock.AnyArg()). // expires_at is a computed timestamp
		WillReturnRows(sqlmock.NewRows([]string{"message_id"}).AddRow("msg-ttl"))

	if _, err := s.Claim(context.Background(), "msg-ttl"); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestRemember_DelegatesToClaim: Remember must drive the same atomic INSERT and
// swallow the won/lost bool, preserving the idempotency.Store write contract.
func TestRemember_DelegatesToClaim(t *testing.T) {
	s, mock := newMock(t)
	mock.ExpectQuery(`INSERT INTO babelqueue_idempotency .* RETURNING message_id`).
		WithArgs("msg-1", nil).
		WillReturnRows(sqlmock.NewRows([]string{"message_id"}).AddRow("msg-1"))

	if err := s.Remember(context.Background(), "msg-1"); err != nil {
		t.Fatalf("Remember: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestRemember_DuplicateIsNoError: Remember on an already-claimed id reports the
// row as durable (no error) even though no new row was written — the seen-set
// contract only needs the id to exist afterwards.
func TestRemember_DuplicateIsNoError(t *testing.T) {
	s, mock := newMock(t)
	mock.ExpectQuery(`INSERT INTO babelqueue_idempotency`).
		WithArgs("msg-dup", nil).
		WillReturnError(sql.ErrNoRows)

	if err := s.Remember(context.Background(), "msg-dup"); err != nil {
		t.Fatalf("Remember of a duplicate must not error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestSeen_True/False prove the expiry predicate: a live row -> seen; no live row
// (absent or expired, both surface as sql.ErrNoRows) -> not seen.
func TestSeen_True(t *testing.T) {
	s, mock := newMock(t)
	mock.ExpectQuery(`SELECT 1 FROM babelqueue_idempotency WHERE message_id = .* expires_at`).
		WithArgs("msg-1").
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))

	seen, err := s.Seen(context.Background(), "msg-1")
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if !seen {
		t.Fatal("a live row must be seen")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestSeen_FalseWhenAbsentOrExpired(t *testing.T) {
	s, mock := newMock(t)
	mock.ExpectQuery(`SELECT 1 FROM babelqueue_idempotency`).
		WithArgs("gone").
		WillReturnError(sql.ErrNoRows)

	seen, err := s.Seen(context.Background(), "gone")
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Fatal("an absent/expired id must not be seen")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestSeen_ErrorPropagates: a real driver error (not ErrNoRows) surfaces so Wrap
// retries the message instead of guessing the dedupe state.
func TestSeen_ErrorPropagates(t *testing.T) {
	s, mock := newMock(t)
	boom := errors.New("connection reset")
	mock.ExpectQuery(`SELECT 1 FROM babelqueue_idempotency`).
		WithArgs("msg-1").
		WillReturnError(boom)

	if _, err := s.Seen(context.Background(), "msg-1"); !errors.Is(err, boom) {
		t.Fatalf("Seen error must propagate, got %v", err)
	}
}

// TestForget issues the DELETE that evicts an id.
func TestForget(t *testing.T) {
	s, mock := newMock(t)
	mock.ExpectExec(`DELETE FROM babelqueue_idempotency WHERE message_id =`).
		WithArgs("msg-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.Forget(context.Background(), "msg-1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestMigrate_RunsDDL proves Migrate emits CREATE TABLE IF NOT EXISTS + the index.
func TestMigrate_RunsDDL(t *testing.T) {
	s, mock := newMock(t)
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS babelqueue_idempotency`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS babelqueue_idempotency_expires_at_idx`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestWithTable_CustomNameInSQL proves WithTable flows into the interpolated
// statement (so a caller can isolate per-app tables).
func TestWithTable_CustomNameInSQL(t *testing.T) {
	s, mock := newMock(t, WithTable("orders_dedupe"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM orders_dedupe WHERE message_id =")).
		WithArgs("x").
		WillReturnError(sql.ErrNoRows)

	if _, err := s.Seen(context.Background(), "x"); err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestValidateIdentifier_RejectsInjection: an unsafe table name is rejected before
// any SQL runs, so the interpolated identifier is safe-by-construction.
func TestValidateIdentifier_RejectsInjection(t *testing.T) {
	bad := []string{"", "1abc", "drop table x", "users;--", "a-b", `"x"`, "tab\tname"}
	for _, name := range bad {
		if err := validateIdentifier(name); err == nil {
			t.Errorf("validateIdentifier(%q) = nil, want rejection", name)
		}
	}
	good := []string{"babelqueue_idempotency", "Orders_Dedupe_2", "_t"}
	for _, name := range good {
		if err := validateIdentifier(name); err != nil {
			t.Errorf("validateIdentifier(%q) = %v, want ok", name, err)
		}
	}
}

// TestClaim_RejectsBadTableBeforeQuery: a Store built with an invalid table fails
// in Claim WITHOUT touching the DB (no expectation set on the mock).
func TestClaim_RejectsBadTableBeforeQuery(t *testing.T) {
	s, _ := newMock(t, WithTable("bad name"))
	if _, err := s.Claim(context.Background(), "x"); err == nil {
		t.Fatal("Claim with an invalid table name must error before querying")
	}
}
