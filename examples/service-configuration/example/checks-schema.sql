CREATE TABLE IF NOT EXISTS jobs (
    audit_id TEXT PRIMARY KEY,
    url TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS results (
    audit_id TEXT PRIMARY KEY REFERENCES jobs (audit_id),
    result_id TEXT NOT NULL UNIQUE,
    findings JSONB NOT NULL
);
