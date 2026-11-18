package auditdemo

import (
	"context"
	"errors"
	"net/url"
)

type Service struct {
	Store   Store
	Checker *Checker
}

func (s Service) Create(ctx context.Context, ownerID int64, key, target string) (Audit, error) {
	u, err := url.Parse(target)
	if err != nil || !s.Checker.Allowed(u) || key == "" || len(key) > 128 {
		return Audit{}, ErrInvalid
	}
	audit, err := s.Store.Create(ctx, ownerID, key, target)
	if err != nil {
		return Audit{}, err
	}
	if _, err := s.Store.Result(ctx, audit.ID); err == nil {
		return audit, nil
	} else if !errors.Is(err, ErrNotFound) {
		return Audit{}, err
	}
	findings, err := s.Checker.Check(ctx, audit.URL)
	if err != nil {
		return Audit{}, err
	}
	resultID, err := newID()
	if err != nil {
		return Audit{}, err
	}
	result := Result{ID: resultID, AuditID: audit.ID, Findings: findings}
	if err := s.Store.SaveResult(ctx, result); err != nil {
		return Audit{}, err
	}
	return audit, nil
}

func (s Service) Report(ctx context.Context, ownerID int64, id string) (Report, error) {
	return s.Store.Report(ctx, ownerID, id)
}
