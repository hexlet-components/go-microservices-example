package audit

import (
	"context"
	"errors"
)

var (
	ErrNotFound = errors.New("audit not found")
	ErrNotReady = errors.New("result not ready")
)

type Finding struct {
	PageURL string `json:"page_url"`
	Code    string `json:"code"`
}

type Result struct {
	ID       string    `json:"result_id"`
	AuditID  string    `json:"audit_id"`
	Findings []Finding `json:"findings"`
}

type CheckState string

const (
	CheckUnknown CheckState = "unknown"
	CheckRunning CheckState = "running"
	CheckDone    CheckState = "done"
)

type Reader interface {
	Result(context.Context, string) (Result, error)
}

type FixtureReader struct{}

func (FixtureReader) Result(ctx context.Context, id string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	switch id {
	case "a-41":
		return Result{ID: "r-41", AuditID: id, Findings: []Finding{{PageURL: "http://127.0.0.1:8090/", Code: "missing-content-security-policy"}}}, nil
	case "a-42":
		return Result{}, ErrNotReady
	default:
		return Result{}, ErrNotFound
	}
}

func (FixtureReader) State(ctx context.Context, id string) (CheckState, error) {
	if err := ctx.Err(); err != nil {
		return CheckUnknown, err
	}
	switch id {
	case "a-41":
		return CheckDone, nil
	case "a-42":
		return CheckRunning, nil
	default:
		return CheckUnknown, ErrNotFound
	}
}
