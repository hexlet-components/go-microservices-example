package release

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

//go:embed schema.sql
var Schema string

type Store struct {
	DB          *sql.DB
	Version     string
	RuleVersion string
}

func (s Store) Prepare(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, Schema)
	return err
}
func (s Store) Ready(ctx context.Context) error {
	rows, err := s.DB.QueryContext(ctx, `SELECT findings,issues FROM results LIMIT 0`)
	if err != nil {
		return err
	}
	return rows.Close()
}
func (s Store) Fixture(ctx context.Context, id string) error {
	if id == "" || len(id) > 64 {
		return fmt.Errorf("invalid fixture ID")
	}
	findings := []Finding{{PageURL: "http://127.0.0.1:8090/", Code: "missing-content-security-policy"}}
	data, err := json.Marshal(findings)
	if err != nil {
		return err
	}
	event := EventV1{EventID: "e-" + id, AuditID: id, ResultID: "r-" + id, Findings: findings}
	var payload []byte
	if s.Version == "v2" {
		version := s.RuleVersion
		payload, err = json.Marshal(EventV2{EventV1: event, RuleVersion: &version})
	} else {
		payload, err = json.Marshal(event)
	}
	if err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	query := `INSERT INTO results(audit_id,findings) VALUES($1,$2) ON CONFLICT(audit_id) DO NOTHING`
	if s.Version == "v2" {
		query = `INSERT INTO results(audit_id,findings,issues) VALUES($1,$2,$2) ON CONFLICT(audit_id) DO NOTHING`
	}
	if _, err := tx.ExecContext(ctx, query, id, data); err != nil {
		return err
	}
	// Only the first terminal result creates the corresponding outbox event.
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox(event_id,audit_id,payload) VALUES($1,$2,$3) ON CONFLICT(event_id) DO NOTHING`, event.EventID, id, payload)
	if err != nil {
		return err
	}
	return tx.Commit()
}
func (s Store) Read(ctx context.Context, id string) ([]Finding, error) {
	var data []byte
	if err := s.DB.QueryRowContext(ctx, `SELECT findings FROM results WHERE audit_id=$1`, id).Scan(&data); err != nil {
		return nil, err
	}
	var findings []Finding
	err := json.Unmarshal(data, &findings)
	return findings, err
}
func (s Store) Relay(ctx context.Context, client *kgo.Client, topic string) error {
	var eventID, auditID string
	var data []byte
	err := s.DB.QueryRowContext(ctx, `SELECT event_id,audit_id,payload FROM outbox WHERE NOT published ORDER BY event_id LIMIT 1`).Scan(&eventID, &auditID, &data)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(auditID), Value: data}).FirstErr(); err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, `UPDATE outbox SET published=TRUE WHERE event_id=$1`, eventID)
	return err
}

func (s Store) StoredRuleVersion(ctx context.Context, id string) (*string, error) {
	var version sql.NullString
	err := s.DB.QueryRowContext(ctx, `SELECT payload->>'rule_version' FROM outbox WHERE audit_id=$1`, id).Scan(&version)
	if err != nil {
		return nil, err
	}
	if !version.Valid {
		return nil, nil
	}
	return &version.String, nil
}
