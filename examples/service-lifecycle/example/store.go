package lifecycle

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

//go:embed schema.sql
var schemaSQL string

var schemaName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

type Finding struct {
	PageURL string `json:"page_url"`
	Code    string `json:"code"`
}

type AuditChecked struct {
	EventID  string    `json:"event_id"`
	AuditID  string    `json:"audit_id"`
	ResultID string    `json:"result_id"`
	Findings []Finding `json:"findings"`
}

type Store struct {
	DB *sql.DB
	// LockKey is used only by the prepared interruption test.
	LockKey int64
	Phase   func(string)
}

func Prepare(ctx context.Context, dsn, schema string) (*sql.DB, error) {
	if !schemaName.MatchString(schema) {
		return nil, errors.New("invalid schema name")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	admin := stdlib.OpenDB(*config)
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+schema); err != nil {
		return nil, err
	}
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (s Store) Apply(ctx context.Context, consumer string, event AuditChecked) (bool, error) {
	if consumer == "" || event.EventID == "" || event.AuditID == "" || event.ResultID == "" {
		return false, errors.New("consumer and event identifiers are required")
	}
	data, err := json.Marshal(event.Findings)
	if err != nil {
		return false, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	marker, err := tx.ExecContext(ctx, `
        INSERT INTO processed_events(consumer, event_id)
        VALUES ($1, $2)
        ON CONFLICT (consumer, event_id) DO NOTHING`, consumer, event.EventID)
	if err != nil {
		return false, err
	}
	inserted, err := marker.RowsAffected()
	if err != nil {
		return false, err
	}
	if inserted == 0 {
		return false, tx.Commit()
	}
	if s.Phase != nil {
		s.Phase("report-persist")
	}
	if s.LockKey != 0 {
		if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", s.LockKey); err != nil {
			return false, err
		}
	}
	_, err = tx.ExecContext(ctx, `
        INSERT INTO reports(audit_id, result_id, findings)
        VALUES ($1, $2, $3)
        ON CONFLICT (audit_id) DO UPDATE
        SET result_id = EXCLUDED.result_id,
            findings = EXCLUDED.findings,
            applications = reports.applications + 1`, event.AuditID, event.ResultID, data)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s Store) Statistics(ctx context.Context, auditID string) (applications int, markers int, err error) {
	err = s.DB.QueryRowContext(ctx, `
        SELECT COALESCE((SELECT applications FROM reports WHERE audit_id=$1), 0),
               (SELECT count(*) FROM processed_events)`, auditID).Scan(&applications, &markers)
	return
}

func DropSchema(ctx context.Context, dsn, schema string) error {
	if !schemaName.MatchString(schema) {
		return fmt.Errorf("invalid schema name")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return err
	}
	db := stdlib.OpenDB(*config)
	defer db.Close()
	_, err = db.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE")
	return err
}
