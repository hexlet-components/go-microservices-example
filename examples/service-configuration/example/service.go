package auditdemo

import (
	"context"
	"net/url"
)

type CheckState string

const (
	CheckUnknown CheckState = "unknown"
	CheckRunning CheckState = "running"
	CheckDone    CheckState = "done"
)

type Checks interface {
	Start(context.Context, Audit) error
	Result(context.Context, string) (Result, error)
	State(context.Context, string) (CheckState, error)
}

type Service struct {
	Store  Store
	Checks Checks
	Origin *url.URL
}

func (s Service) Create(ctx context.Context, ownerID int64, key, target string) (Audit, error) {
	u, err := url.Parse(target)
	if err != nil || u.User != nil || u.Scheme != s.Origin.Scheme || u.Host != s.Origin.Host || key == "" || len(key) > 128 {
		return Audit{}, ErrInvalid
	}
	audit, err := s.Store.Create(ctx, ownerID, key, target)
	if err != nil {
		return Audit{}, err
	}
	if err := s.Checks.Start(ctx, audit); err != nil {
		return Audit{}, err
	}
	result, err := s.Checks.Result(ctx, audit.ID)
	if err != nil {
		return Audit{}, err
	}
	if err := s.Store.SaveReport(ctx, result); err != nil {
		return Audit{}, err
	}
	return audit, nil
}

func (s Service) Report(ctx context.Context, ownerID int64, id string) (Report, error) {
	return s.Store.Report(ctx, ownerID, id)
}
