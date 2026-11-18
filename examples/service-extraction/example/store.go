package auditdemo

import (
	"context"
	"crypto/rand"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
)

//go:embed schema.sql
var schema string

type Store struct{ DB *sql.DB }

func (s Store) Migrate(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, schema)
	return err
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (s Store) Create(ctx context.Context, ownerID int64, key, target string) (Audit, error) {
	id, err := newID()
	if err != nil {
		return Audit{}, err
	}
	_, err = s.DB.ExecContext(ctx, `INSERT INTO audits (audit_id,owner_id,url,idempotency_key)
 VALUES ($1,$2,$3,$4) ON CONFLICT (owner_id,idempotency_key) DO NOTHING`, id, ownerID, target, key)
	if err != nil {
		return Audit{}, err
	}
	var audit Audit
	err = s.DB.QueryRowContext(ctx, `SELECT audit_id,owner_id,url FROM audits
 WHERE owner_id=$1 AND idempotency_key=$2`, ownerID, key).Scan(&audit.ID, &audit.OwnerID, &audit.URL)
	if err != nil {
		return Audit{}, err
	}
	if audit.URL != target {
		return Audit{}, ErrConflict
	}
	return audit, nil
}

func (s Store) Audit(ctx context.Context, ownerID int64, id string) (Audit, error) {
	var audit Audit
	err := s.DB.QueryRowContext(ctx, `SELECT audit_id,owner_id,url FROM audits WHERE audit_id=$1 AND owner_id=$2`, id, ownerID).Scan(&audit.ID, &audit.OwnerID, &audit.URL)
	if errors.Is(err, sql.ErrNoRows) {
		return Audit{}, ErrNotFound
	}
	return audit, err
}

func (s Store) SaveReport(ctx context.Context, result Result) error {
	data, err := json.Marshal(result.Findings)
	if err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, `INSERT INTO reports (audit_id,findings) VALUES ($1,$2) ON CONFLICT (audit_id) DO NOTHING`, result.AuditID, data)
	return err
}

func (s Store) Report(ctx context.Context, ownerID int64, id string) (Report, error) {
	report := Report{AuditID: id, Findings: make([]Finding, 0)}
	var data []byte
	err := s.DB.QueryRowContext(ctx, `SELECT reports.findings FROM audits
 LEFT JOIN reports USING (audit_id) WHERE audit_id=$1 AND owner_id=$2`, id, ownerID).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return Report{}, ErrNotFound
	}
	if err != nil {
		return Report{}, err
	}
	if data == nil {
		return report, nil
	}
	report.Ready = true
	err = json.Unmarshal(data, &report.Findings)
	return report, err
}
