package tracedemo

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"

	"go.opentelemetry.io/otel/propagation"
)

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
type Envelope struct {
	Event   AuditChecked
	Carrier propagation.MapCarrier
}
type Outbox interface {
	Save(context.Context, Envelope) error
	Load(context.Context, string) (Envelope, error)
	MarkSent(context.Context, string) error
}

type SQLStore struct{ DB *sql.DB }

func (s SQLStore) Migrate(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS trace_results(audit_id TEXT PRIMARY KEY,result_id TEXT UNIQUE NOT NULL,payload JSONB NOT NULL);CREATE TABLE IF NOT EXISTS trace_outbox(event_id TEXT PRIMARY KEY,payload JSONB NOT NULL,carrier JSONB NOT NULL,sent_at TIMESTAMPTZ)`)
	return err
}
func (s SQLStore) Save(ctx context.Context, e Envelope) error {
	payload, err := json.Marshal(e.Event)
	if err != nil {
		return err
	}
	carrier, err := json.Marshal(e.Carrier)
	if err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO trace_results(audit_id,result_id,payload) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, e.Event.AuditID, e.Event.ResultID, payload)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO trace_outbox(event_id,payload,carrier) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, e.Event.EventID, payload, carrier)
	if err != nil {
		return err
	}
	return tx.Commit()
}
func (s SQLStore) Load(ctx context.Context, id string) (Envelope, error) {
	var e Envelope
	var payload, carrier []byte
	err := s.DB.QueryRowContext(ctx, `SELECT payload,carrier FROM trace_outbox WHERE event_id=$1`, id).Scan(&payload, &carrier)
	if err != nil {
		return e, err
	}
	if err := json.Unmarshal(payload, &e.Event); err != nil {
		return e, err
	}
	err = json.Unmarshal(carrier, &e.Carrier)
	return e, err
}
func (s SQLStore) MarkSent(ctx context.Context, id string) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE trace_outbox SET sent_at=now() WHERE event_id=$1`, id)
	return err
}

// MemoryStore is a prepared outbox snapshot for transport tests; CLI uses SQLStore.
type MemoryStore struct {
	mu     sync.Mutex
	Events map[string]Envelope
	Sent   map[string]bool
}

func (s *MemoryStore) Save(_ context.Context, e Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Events == nil {
		s.Events = map[string]Envelope{}
	}
	if _, exists := s.Events[e.Event.EventID]; !exists {
		s.Events[e.Event.EventID] = e
	}
	return nil
}
func (s *MemoryStore) Load(_ context.Context, id string) (Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Events[id], nil
}
func (s *MemoryStore) MarkSent(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Sent == nil {
		s.Sent = map[string]bool{}
	}
	s.Sent[id] = true
	return nil
}
