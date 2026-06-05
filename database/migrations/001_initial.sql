-- Financial events table.
-- Every event that comes through Kafka lands here permanently.
--
-- Think of this like a ledger. Every transaction that happens
-- gets written down here and never deleted. This is the record
-- of everything that occurred in the system.
--
-- event_id is the most important column. It is a unique ID that
-- the producer stamps on every event before sending it to Kafka.
-- If the same event arrives twice (which can happen with
-- at-least-once delivery), the second insert sees that event_id
-- already exists and does nothing instead of creating a duplicate row.
-- That behaviour is what makes this system safe for financial data.

CREATE TABLE IF NOT EXISTS events (
    id                  BIGSERIAL PRIMARY KEY,
    event_id            UUID NOT NULL,
    event_type          VARCHAR(50) NOT NULL,
    account_id          VARCHAR(50) NOT NULL,
    amount              NUMERIC(18, 2) NOT NULL,
    currency            VARCHAR(3) NOT NULL DEFAULT 'USD',
    status              VARCHAR(20) NOT NULL DEFAULT 'processed',
    payload             JSONB NOT NULL,
    kafka_topic         VARCHAR(100) NOT NULL,
    kafka_partition     INTEGER NOT NULL,
    kafka_offset        BIGINT NOT NULL,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT events_event_id_unique UNIQUE (event_id)
);

-- These three indexes exist for one reason: speed.
--
-- Without indexes, PostgreSQL reads every single row in the table
-- to find what you are looking for. At 10,000 events per minute
-- that table grows to 600,000 rows per hour. Reading all of that
-- every time someone queries by account is too slow.
--
-- An index is like the index at the back of a book. Instead of
-- reading every page to find a topic, you go to the index,
-- find the page number, and jump straight there.

CREATE INDEX IF NOT EXISTS idx_events_account_id
    ON events(account_id);

CREATE INDEX IF NOT EXISTS idx_events_processed_at
    ON events(processed_at);

CREATE INDEX IF NOT EXISTS idx_events_event_type
    ON events(event_type);


-- Dead letter queue table.
-- When an event fails processing 3 times in a row, it lands here.
--
-- Think of this like a hospital triage area for broken events.
-- Instead of throwing them away or blocking the whole system,
-- we park them here safely so someone can investigate later.
--
-- Without this table, one bad event could block an entire Kafka
-- partition forever. With it, the bad event gets moved here
-- after 3 attempts and the rest of the stream continues normally.

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id                  BIGSERIAL PRIMARY KEY,
    event_id            UUID NOT NULL,
    event_type          VARCHAR(50),
    account_id          VARCHAR(50),
    payload             JSONB NOT NULL,
    kafka_topic         VARCHAR(100) NOT NULL,
    kafka_partition     INTEGER NOT NULL,
    kafka_offset        BIGINT NOT NULL,
    failure_reason      TEXT NOT NULL,
    retry_count         INTEGER NOT NULL DEFAULT 3,
    failed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved            BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at         TIMESTAMPTZ
);

-- resolved column index.
-- The operations team queries the DLQ looking for unresolved failures.
-- This index makes that query fast instead of scanning every row.

CREATE INDEX IF NOT EXISTS idx_dlq_resolved
    ON dead_letter_queue(resolved);

CREATE INDEX IF NOT EXISTS idx_dlq_failed_at
    ON dead_letter_queue(failed_at);