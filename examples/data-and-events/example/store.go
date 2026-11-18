package auditdemo

import (
	"context"
	"crypto/rand"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

//go:embed schema.sql
var outboxSchema string

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
		"api":     `CREATE TABLE IF NOT EXISTS audits(audit_id TEXT PRIMARY KEY,owner_id BIGINT NOT NULL,url TEXT NOT NULL,idempotency_key TEXT NOT NULL,event_id TEXT NOT NULL UNIQUE,UNIQUE(owner_id,idempotency_key))`,
		"checks":  `CREATE TABLE IF NOT EXISTS results(audit_id TEXT PRIMARY KEY,result_id TEXT NOT NULL UNIQUE,event_id TEXT NOT NULL UNIQUE,findings JSONB NOT NULL)`,
		"reports": `CREATE TABLE IF NOT EXISTS processed_events(event_id TEXT PRIMARY KEY);CREATE TABLE IF NOT EXISTS reports(audit_id TEXT PRIMARY KEY,result_id TEXT NOT NULL,findings JSONB NOT NULL,applications INT NOT NULL DEFAULT 1)`,
	}
	schema, ok := schemas[role]
	if !ok {
		return ErrInvalid
	}
	_, err := s.DB.ExecContext(ctx, schema+";"+outboxSchema)
	return err
}

func enqueue(ctx context.Context, tx *sql.Tx, eventID, auditID, topic string, event any) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox(event_id,audit_id,topic,payload) VALUES($1,$2,$3,$4)`, eventID, auditID, topic, data)
	return err
}

func (s Store) Create(ctx context.Context, owner int64, key, target, topic string) (AuditCreated, error) {
	if owner <= 0 || key == "" || target == "" || topic == "" {
		return AuditCreated{}, ErrInvalid
	}
	id, err := newID()
	if err != nil {
		return AuditCreated{}, err
	}
	event := AuditCreated{EventID: "created-" + id, AuditID: id, URL: target}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return AuditCreated{}, err
	}
	defer tx.Rollback()
	var inserted string
	err = tx.QueryRowContext(ctx, `INSERT INTO audits(audit_id,owner_id,url,idempotency_key,event_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT(owner_id,idempotency_key) DO NOTHING RETURNING audit_id`, id, owner, target, key, event.EventID).Scan(&inserted)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Rollback(); err != nil {
			return AuditCreated{}, err
		}
		var existing AuditCreated
		err = s.DB.QueryRowContext(ctx, `SELECT event_id,audit_id,url FROM audits WHERE owner_id=$1 AND idempotency_key=$2`, owner, key).Scan(&existing.EventID, &existing.AuditID, &existing.URL)
		if err == nil && existing.URL != target {
			return AuditCreated{}, ErrConflict
		}
		return existing, err
	}
	if err != nil {
		return AuditCreated{}, err
	}
	if err := enqueue(ctx, tx, event.EventID, event.AuditID, topic, event); err != nil {
		return AuditCreated{}, err
	}
	if err := tx.Commit(); err != nil {
		return AuditCreated{}, err
	}
	return event, nil
}

func (s Store) SaveCheck(ctx context.Context, auditID string, findings []Finding, topic string) (AuditChecked, error) {
	if auditID == "" || topic == "" {
		return AuditChecked{}, ErrInvalid
	}
	resultID, err := newID()
	if err != nil {
		return AuditChecked{}, err
	}
	event := AuditChecked{EventID: "checked-" + resultID, AuditID: auditID, ResultID: resultID, Findings: findings}
	data, err := json.Marshal(findings)
	if err != nil {
		return AuditChecked{}, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return AuditChecked{}, err
	}
	defer tx.Rollback()
	var inserted string
	err = tx.QueryRowContext(ctx, `INSERT INTO results(audit_id,result_id,event_id,findings) VALUES($1,$2,$3,$4) ON CONFLICT(audit_id) DO NOTHING RETURNING result_id`, auditID, resultID, event.EventID, data).Scan(&inserted)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Rollback(); err != nil {
			return AuditChecked{}, err
		}
		return s.Result(ctx, auditID)
	}
	if err != nil {
		return AuditChecked{}, err
	}
	if err := enqueue(ctx, tx, event.EventID, auditID, topic, event); err != nil {
		return AuditChecked{}, err
	}
	if err := tx.Commit(); err != nil {
		return AuditChecked{}, err
	}
	return event, nil
}

func (s Store) Result(ctx context.Context, auditID string) (AuditChecked, error) {
	var e AuditChecked
	var data []byte
	err := s.DB.QueryRowContext(ctx, `SELECT audit_id,result_id,event_id,findings FROM results WHERE audit_id=$1`, auditID).Scan(&e.AuditID, &e.ResultID, &e.EventID, &data)
	if errors.Is(err, sql.ErrNoRows) {
		return e, ErrNotFound
	}
	if err != nil {
		return e, err
	}
	err = json.Unmarshal(data, &e.Findings)
	return e, err
}

type Envelope struct {
	EventID string
	AuditID string
	Topic   string
	Payload []byte
}

func (s Store) Next(ctx context.Context) (Envelope, bool, error) {
	var e Envelope
	err := s.DB.QueryRowContext(ctx, `SELECT event_id,audit_id,topic,payload FROM outbox WHERE sent_at IS NULL ORDER BY created_at,event_id LIMIT 1`).Scan(&e.EventID, &e.AuditID, &e.Topic, &e.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return e, false, nil
	}
	return e, err == nil, err
}
func (s Store) MarkSent(ctx context.Context, eventID string) error {
	result, err := s.DB.ExecContext(ctx, `UPDATE outbox SET sent_at=now() WHERE event_id=$1 AND sent_at IS NULL`, eventID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err == nil && rows != 1 {
		return fmt.Errorf("outbox event %s is missing or already marked", eventID)
	}
	return err
}
func (s Store) Pending(ctx context.Context) (int, error) {
	var count int
	err := s.DB.QueryRowContext(ctx, `SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&count)
	return count, err
}

// ApplyReport is the prepared idempotent consumer from the same application.
func (s Store) ApplyReport(ctx context.Context, e AuditChecked) error {
	data, err := json.Marshal(e.Findings)
	if err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO processed_events(event_id) VALUES($1) ON CONFLICT DO NOTHING`, e.EventID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return tx.Commit()
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO reports(audit_id,result_id,findings) VALUES($1,$2,$3) ON CONFLICT(audit_id) DO UPDATE SET result_id=EXCLUDED.result_id,findings=EXCLUDED.findings,applications=reports.applications+1`, e.AuditID, e.ResultID, data)
	if err != nil {
		return err
	}
	return tx.Commit()
}
