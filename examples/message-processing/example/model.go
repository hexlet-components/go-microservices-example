package auditdemo

import "errors"

var (
	ErrNotFound = errors.New("audit not found")
	ErrConflict = errors.New("idempotency key has another request")
	ErrInvalid  = errors.New("invalid request")
)

type Audit struct {
	ID      string `json:"audit_id"`
	OwnerID int64  `json:"-"`
	URL     string `json:"url"`
}

type Finding struct {
	PageURL string `json:"page_url"`
	Code    string `json:"code"`
}

type Result struct {
	ID       string    `json:"result_id"`
	AuditID  string    `json:"audit_id"`
	Findings []Finding `json:"findings"`
}

type Report struct {
	AuditID  string    `json:"audit_id"`
	Ready    bool      `json:"ready"`
	Findings []Finding `json:"findings"`
}
