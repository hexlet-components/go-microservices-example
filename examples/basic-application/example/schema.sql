CREATE TABLE IF NOT EXISTS audits (
    audit_id TEXT PRIMARY KEY,
    owner_id BIGINT NOT NULL,
    url TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    UNIQUE (owner_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS results (
    audit_id TEXT PRIMARY KEY REFERENCES audits (audit_id),
    result_id TEXT NOT NULL UNIQUE,
    findings JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS reports (
    audit_id TEXT PRIMARY KEY REFERENCES audits (audit_id),
    findings JSONB NOT NULL
);
