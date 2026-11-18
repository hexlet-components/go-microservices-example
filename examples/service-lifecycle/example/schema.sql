CREATE TABLE IF NOT EXISTS processed_events (
    consumer TEXT NOT NULL,
    event_id TEXT NOT NULL,
    PRIMARY KEY (consumer, event_id)
);

CREATE TABLE IF NOT EXISTS reports (
    audit_id TEXT PRIMARY KEY,
    result_id TEXT NOT NULL,
    findings JSONB NOT NULL,
    applications INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS schema_version (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL);
INSERT INTO schema_version(id, version) VALUES (1, 1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS results (audit_id TEXT PRIMARY KEY, event_id TEXT UNIQUE NOT NULL, result_id TEXT NOT NULL, findings JSONB NOT NULL);
CREATE TABLE IF NOT EXISTS outbox (event_id TEXT PRIMARY KEY, audit_id TEXT NOT NULL, payload JSONB NOT NULL, published BOOLEAN NOT NULL DEFAULT FALSE);
