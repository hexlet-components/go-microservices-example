package auditdemo

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"net/url"
)

//go:embed checks-schema.sql
var checksSchema string

type CheckStore struct{ DB *sql.DB }

func (s CheckStore) Migrate(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, checksSchema)
	return err
}

func (s CheckStore) Register(ctx context.Context, audit Audit) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO jobs (audit_id,url) VALUES ($1,$2) ON CONFLICT (audit_id) DO NOTHING`, audit.ID, audit.URL)
	if err != nil {
		return err
	}
	var target string
	if err := s.DB.QueryRowContext(ctx, `SELECT url FROM jobs WHERE audit_id=$1`, audit.ID).Scan(&target); err != nil {
		return err
	}
	if target != audit.URL {
		return ErrConflict
	}
	return nil
}

func (s CheckStore) Result(ctx context.Context, id string) (Result, error) {
	var r Result
	var data []byte
	err := s.DB.QueryRowContext(ctx, `SELECT result_id,audit_id,findings FROM results WHERE audit_id=$1`, id).Scan(&r.ID, &r.AuditID, &data)
	if errors.Is(err, sql.ErrNoRows) {
		var exists bool
		if err := s.DB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM jobs WHERE audit_id=$1)`, id).Scan(&exists); err != nil {
			return Result{}, err
		}
		if exists {
			return Result{}, ErrNotReady
		}
		return Result{}, ErrNotFound
	}
	if err != nil {
		return Result{}, err
	}
	err = json.Unmarshal(data, &r.Findings)
	return r, err
}

func (s CheckStore) SaveResult(ctx context.Context, r Result) error {
	data, err := json.Marshal(r.Findings)
	if err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, `INSERT INTO results (audit_id,result_id,findings) VALUES ($1,$2,$3) ON CONFLICT (audit_id) DO NOTHING`, r.AuditID, r.ID, data)
	return err
}

type CheckService struct {
	Store   CheckStore
	Checker *Checker
}

func (s CheckService) Start(ctx context.Context, audit Audit) error {
	u, err := url.Parse(audit.URL)
	if audit.ID == "" || err != nil || !s.Checker.Allowed(u) {
		return ErrInvalid
	}
	if err := s.Store.Register(ctx, audit); err != nil {
		return err
	}
	if _, err := s.Store.Result(ctx, audit.ID); err == nil {
		return nil
	} else if !errors.Is(err, ErrNotReady) {
		return err
	}
	findings, err := s.Checker.Check(ctx, audit.URL)
	if err != nil {
		return err
	}
	id, err := newID()
	if err != nil {
		return err
	}
	return s.Store.SaveResult(ctx, Result{ID: id, AuditID: audit.ID, Findings: findings})
}
func (s CheckService) Result(ctx context.Context, id string) (Result, error) {
	return s.Store.Result(ctx, id)
}
func (s CheckService) State(ctx context.Context, id string) (CheckState, error) {
	_, err := s.Result(ctx, id)
	if errors.Is(err, ErrNotReady) {
		return CheckRunning, nil
	}
	if err != nil {
		return "", err
	}
	return CheckDone, nil
}
