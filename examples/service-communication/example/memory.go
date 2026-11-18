package audit

import "context"

type MemoryChecks struct {
	audits  map[string]Audit
	results map[string]Result
}

func NewMemoryChecks() *MemoryChecks {
	return &MemoryChecks{audits: map[string]Audit{}, results: map[string]Result{}}
}
func (m *MemoryChecks) Start(ctx context.Context, a Audit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.audits[a.ID] = a
	return nil
}
func (m *MemoryChecks) State(ctx context.Context, id string) (CheckState, error) {
	if err := ctx.Err(); err != nil {
		return CheckUnknown, err
	}
	if _, ok := m.audits[id]; !ok {
		return CheckUnknown, ErrNotFound
	}
	if _, ok := m.results[id]; ok {
		return CheckDone, nil
	}
	return CheckRunning, nil
}
func (m *MemoryChecks) Result(ctx context.Context, id string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if _, ok := m.audits[id]; !ok {
		return Result{}, ErrNotFound
	}
	r, ok := m.results[id]
	if !ok {
		return Result{}, ErrNotReady
	}
	r.Findings = append([]Finding(nil), r.Findings...)
	return r, nil
}
func (m *MemoryChecks) Complete(r Result) { m.results[r.AuditID] = r }

var _ Checks = (*MemoryChecks)(nil)
