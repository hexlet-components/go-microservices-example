package audit

import (
	"context"
	"errors"
)

var (
	ErrNotFound = errors.New("audit not found")
	ErrNotReady = errors.New("result not ready")
)

type Audit struct {
	ID  string
	URL string
}

type Finding struct {
	PageURL string `json:"page_url"`
	Code    string `json:"code"`
}

type Result struct {
	ID       string
	AuditID  string
	Findings []Finding
}

type CheckState string

const (
	CheckUnknown CheckState = "unknown"
	CheckRunning CheckState = "running"
	CheckDone    CheckState = "done"
)

type Checks interface {
	Start(context.Context, Audit) error
	State(context.Context, string) (CheckState, error)
	Result(context.Context, string) (Result, error)
}
