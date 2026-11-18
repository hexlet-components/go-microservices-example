CREATE TABLE IF NOT EXISTS outbox (
    event_id TEXT PRIMARY KEY,
    audit_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS pending_outbox ON outbox (created_at, event_id) WHERE sent_at IS NULL;
