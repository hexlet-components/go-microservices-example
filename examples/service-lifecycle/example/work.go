package lifecycle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/twmb/franz-go/pkg/kgo"
)

var ErrInvalid = errors.New("invalid request")

type AuditCreated struct {
	EventID string `json:"event_id"`
	AuditID string `json:"audit_id"`
	URL     string `json:"url"`
}

func Connect(ctx context.Context, dsn, schema string) (*sql.DB, error) {
	if !schemaName.MatchString(schema) {
		return nil, errors.New("invalid schema")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	var version int
	if err := db.QueryRowContext(ctx, "SELECT version FROM schema_version WHERE id=1").Scan(&version); err != nil {
		db.Close()
		return nil, err
	}
	if version != 1 {
		db.Close()
		return nil, errors.New("unsupported database schema version")
	}
	return db, nil
}

func (s Store) check(ctx context.Context, client *kgo.Client, topic string, checker *Checker, input AuditCreated) error {
	var event AuditChecked
	var data []byte
	err := s.DB.QueryRowContext(ctx, "SELECT payload FROM outbox WHERE audit_id=$1", input.AuditID).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		if s.Phase != nil {
			s.Phase("checking")
		}
		findings, err := checker.Check(ctx, input.URL)
		if err != nil {
			return err
		}
		event = AuditChecked{EventID: "checked-" + input.AuditID, AuditID: input.AuditID, ResultID: "result-" + input.AuditID, Findings: findings}
		data, err = json.Marshal(event)
		if err != nil {
			return err
		}
		tx, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		findingsJSON, err := json.Marshal(findings)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO results(audit_id,event_id,result_id,findings) VALUES($1,$2,$3,$4) ON CONFLICT(audit_id) DO NOTHING", event.AuditID, event.EventID, event.ResultID, findingsJSON); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO outbox(event_id,audit_id,payload) VALUES($1,$2,$3) ON CONFLICT(event_id) DO NOTHING", event.EventID, event.AuditID, data); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if err := json.Unmarshal(data, &event); err != nil {
		return err
	}
	if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(event.AuditID), Value: data}).FirstErr(); err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, "UPDATE outbox SET published=TRUE WHERE event_id=$1", event.EventID)
	return err
}
