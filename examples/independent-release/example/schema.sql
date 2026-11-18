CREATE TABLE IF NOT EXISTS results (
    audit_id TEXT PRIMARY KEY,
    findings JSONB NOT NULL,
    issues JSONB
);
CREATE TABLE IF NOT EXISTS outbox (
    event_id TEXT PRIMARY KEY,
    audit_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE
);
