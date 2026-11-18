package auditdemo

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
)

type Store struct{ DB *sql.DB }

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (s Store) Migrate(ctx context.Context, role string) error {
	schemas := map[string]string{
		"api":     `CREATE TABLE IF NOT EXISTS audits(audit_id text PRIMARY KEY,owner_id bigint NOT NULL,url text NOT NULL,idempotency_key text NOT NULL,UNIQUE(owner_id,idempotency_key))`,
		"checks":  `CREATE TABLE IF NOT EXISTS results(audit_id text PRIMARY KEY,result_id text NOT NULL UNIQUE,findings jsonb NOT NULL)`,
		"reports": `CREATE TABLE IF NOT EXISTS reports(audit_id text PRIMARY KEY,result_id text NOT NULL,findings jsonb NOT NULL,applications integer NOT NULL DEFAULT 1)`,
	}
	statement, ok := schemas[role]
	if !ok {
		return ErrInvalid
	}
	_, err := s.DB.ExecContext(ctx, statement)
	return err
}

func (s Store) Create(ctx context.Context, owner int64, key, target string) (Audit, error) {
	id, err := newID()
	if err != nil {
		return Audit{}, err
	}
	_, err = s.DB.ExecContext(ctx, `INSERT INTO audits(audit_id,owner_id,url,idempotency_key) VALUES($1,$2,$3,$4) ON CONFLICT(owner_id,idempotency_key) DO NOTHING`, id, owner, target, key)
	if err != nil {
		return Audit{}, err
	}
	var a Audit
	err = s.DB.QueryRowContext(ctx, `SELECT audit_id,owner_id,url FROM audits WHERE owner_id=$1 AND idempotency_key=$2`, owner, key).Scan(&a.ID, &a.OwnerID, &a.URL)
	if err == nil && a.URL != target {
		return Audit{}, ErrConflict
	}
	return a, err
}

func (s Store) Owned(ctx context.Context, owner int64, id string) error {
	var found string
	err := s.DB.QueryRowContext(ctx, `SELECT audit_id FROM audits WHERE audit_id=$1 AND owner_id=$2`, id, owner).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

func (s Store) SaveCheck(ctx context.Context, auditID string, findings []Finding) (AuditChecked, error) {
	id, err := newID()
	if err != nil {
		return AuditChecked{}, err
	}
	data, err := json.Marshal(findings)
	if err != nil {
		return AuditChecked{}, err
	}
	_, err = s.DB.ExecContext(ctx, `INSERT INTO results(audit_id,result_id,findings) VALUES($1,$2,$3) ON CONFLICT(audit_id) DO NOTHING`, auditID, id, data)
	if err != nil {
		return AuditChecked{}, err
	}
	e := AuditChecked{AuditID: auditID}
	err = s.DB.QueryRowContext(ctx, `SELECT result_id,findings FROM results WHERE audit_id=$1`, auditID).Scan(&e.ResultID, &data)
	if err != nil {
		return e, err
	}
	e.EventID = "checked-" + e.ResultID
	err = json.Unmarshal(data, &e.Findings)
	return e, err
}

func (s Store) SaveReport(ctx context.Context, e AuditChecked) error {
	data, err := json.Marshal(e.Findings)
	if err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, `INSERT INTO reports(audit_id,result_id,findings) VALUES($1,$2,$3) ON CONFLICT(audit_id) DO UPDATE SET findings=EXCLUDED.findings,result_id=EXCLUDED.result_id,applications=reports.applications+1`, e.AuditID, e.ResultID, data)
	return err
}

func (s Store) Report(ctx context.Context, id string) (Report, error) {
	r := Report{AuditID: id, Findings: make([]Finding, 0)}
	var data []byte
	err := s.DB.QueryRowContext(ctx, `SELECT findings FROM reports WHERE audit_id=$1`, id).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return r, nil
	}
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(data, &r.Findings)
	r.Ready = err == nil
	return r, err
}
