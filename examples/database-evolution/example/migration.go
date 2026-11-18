package migrationdemo

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
)

//go:embed initial.sql
var Initial string

//go:embed expand.sql
var Expand string

//go:embed backfill.sql
var Backfill string

//go:embed contract.sql
var Contract string

type Finding struct {
	PageURL string `json:"page_url"`
	Code    string `json:"code"`
}
type Result struct {
	AuditID  string    `json:"audit_id"`
	Findings []Finding `json:"findings"`
}
type Store struct {
	DB      *sql.DB
	Version int
	ReadNew bool
}

func (s Store) Write(ctx context.Context, r Result) error {
	data, err := json.Marshal(r.Findings)
	if err != nil {
		return err
	}
	var statement string
	switch s.Version {
	case 1:
		statement = `INSERT INTO results(audit_id,findings) VALUES($1,$2) ON CONFLICT(audit_id) DO UPDATE SET findings=EXCLUDED.findings`
	case 2:
		statement = `INSERT INTO results(audit_id,findings,issues) VALUES($1,$2,$2) ON CONFLICT(audit_id) DO UPDATE SET findings=EXCLUDED.findings,issues=EXCLUDED.issues`
	case 3:
		statement = `INSERT INTO results(audit_id,issues) VALUES($1,$2) ON CONFLICT(audit_id) DO UPDATE SET issues=EXCLUDED.issues`
	default:
		return fmt.Errorf("unsupported version")
	}
	_, err = s.DB.ExecContext(ctx, statement, r.AuditID, data)
	return err
}
func (s Store) Read(ctx context.Context, id string) (Result, error) {
	statement := `SELECT findings FROM results WHERE audit_id=$1`
	if s.ReadNew {
		statement = `SELECT issues FROM results WHERE audit_id=$1`
	}
	r := Result{AuditID: id}
	var data []byte
	if err := s.DB.QueryRowContext(ctx, statement, id).Scan(&data); err != nil {
		return r, err
	}
	err := json.Unmarshal(data, &r.Findings)
	return r, err
}
func (s Store) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /results/{id}", func(w http.ResponseWriter, r *http.Request) {
		var value Result
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&value); err != nil {
			http.Error(w, "invalid", 400)
			return
		}
		value.AuditID = r.PathValue("id")
		if err := s.Write(r.Context(), value); err != nil {
			http.Error(w, "write failed", 503)
			return
		}
		w.WriteHeader(204)
	})
	mux.HandleFunc("GET /results/{id}", func(w http.ResponseWriter, r *http.Request) {
		value, err := s.Read(r.Context(), r.PathValue("id"))
		if err != nil {
			http.Error(w, "read failed", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(value)
	})
	return mux
}
