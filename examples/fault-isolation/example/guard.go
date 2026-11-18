package isolation

import (
	"context"
	"errors"
	"time"

	"github.com/sony/gobreaker/v2"
)

var (
	ErrBusy     = errors.New("dependency concurrency limit reached")
	ErrNotFound = errors.New("audit not found")
	ErrNotReady = errors.New("audit not ready")
)

type Finding struct {
	PageURL string `json:"page_url"`
	Code    string `json:"code"`
}

type Result struct {
	ID       string `json:"result_id"`
	AuditID  string
	Findings []Finding
}
type Reader func(context.Context, string) (Result, error)
type Guard struct {
	slots   chan struct{}
	breaker *gobreaker.CircuitBreaker[Result]
	next    Reader
}

func New(next Reader, limit int, resetAfter time.Duration) *Guard {
	return &Guard{slots: make(chan struct{}, limit), next: next, breaker: gobreaker.NewCircuitBreaker[Result](gobreaker.Settings{
		Name: "checks-read", MaxRequests: 1, Timeout: resetAfter,
		ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= 2 },
		IsSuccessful: func(err error) bool {
			return err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotReady) || errors.Is(err, context.Canceled)
		},
	})}
}

func (g *Guard) Read(ctx context.Context, auditID string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	select {
	case g.slots <- struct{}{}:
		defer func() { <-g.slots }()
	default:
		return Result{}, ErrBusy
	}
	return g.breaker.Execute(func() (Result, error) { return g.next(ctx, auditID) })
}
