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
