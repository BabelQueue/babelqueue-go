-- BabelQueue idempotency store — PostgreSQL DDL (ADR-0022).
--
-- This is the table the postgres.Store reads and writes for consumer-side dedupe
-- on the envelope's meta.id. The Go Store can create it for you with
-- store.Migrate(ctx); ship this file instead when you apply schema through your
-- own migration tool. The table/index names below assume the default table
-- (postgres.DefaultTable = "babelqueue_idempotency"); rename to match WithTable.
--
-- The atomic claim relies on the PRIMARY KEY: INSERT ... ON CONFLICT DO NOTHING
-- (or DO UPDATE for an expired row) makes exactly one of N concurrent consumers
-- win the insert, so duplicate deliveries serialize without an application lock.

CREATE TABLE IF NOT EXISTS babelqueue_idempotency (
    -- The envelope meta.id (canonical per-message identity). The PRIMARY KEY is the
    -- dedupe lock: concurrent INSERTs of the same id race on it; only one wins.
    message_id  TEXT        NOT NULL PRIMARY KEY,

    -- When the id was first claimed (or last re-claimed after expiry).
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- When this id stops being "seen" and may be re-processed. NULL = never expires
    -- (TTL disabled); set to now() + ttl when a TTL is configured. Seen() and the
    -- claim treat a row with expires_at <= now() as absent.
    expires_at  TIMESTAMPTZ NULL
);

-- Supports the periodic GC of expired rows (DELETE ... WHERE expires_at <= now())
-- and keeps Seen()'s expiry predicate efficient. Drop it if you never set a TTL.
CREATE INDEX IF NOT EXISTS babelqueue_idempotency_expires_at_idx
    ON babelqueue_idempotency (expires_at);

-- Optional housekeeping — run periodically (cron / pg_cron) to reclaim space from
-- ids that have lapsed. Expired rows are already ignored by Seen()/Claim(), so this
-- is purely a space reclaim, not a correctness requirement:
--   DELETE FROM babelqueue_idempotency WHERE expires_at IS NOT NULL AND expires_at <= now();
