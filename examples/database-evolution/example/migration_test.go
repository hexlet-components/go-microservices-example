package migrationdemo

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func database(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("AUDIT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set AUDIT_TEST_DATABASE_URL for PostgreSQL integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	var b [8]byte
	if _, err = rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	name := "migration_" + hex.EncodeToString(b[:])
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err = admin.ExecContext(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.RuntimeParams["search_path"] = name
	db := sql.OpenDB(stdlib.GetConnector(*cfg))
	t.Cleanup(func() {
		db.Close()
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		admin.ExecContext(cleanup, "DROP SCHEMA "+quoted+" CASCADE")
		admin.Close()
	})
	if _, err = db.ExecContext(ctx, Initial); err != nil {
		t.Fatal(err)
	}
	return db
}
func exec(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, statement); err != nil {
		t.Fatal(err)
	}
}
func put(t *testing.T, address string, findings []Finding) {
	t.Helper()
	data, err := json.Marshal(Result{AuditID: "a-42", Findings: findings})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPut, address+"/results/a-42", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	r, err := (&http.Client{Timeout: time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 204 {
		body, _ := io.ReadAll(r.Body)
		t.Fatalf("PUT status=%d body=%s", r.StatusCode, body)
	}
}
func get(t *testing.T, address string) Result {
	t.Helper()
	r, err := (&http.Client{Timeout: time.Second}).Get(address + "/results/a-42")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		t.Fatalf("GET status=%d", r.StatusCode)
	}
	var value Result
	if err = json.NewDecoder(r.Body).Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestMixedVersionsAndRollback(t *testing.T) {
	db := database(t)
	ctx := context.Background()
	old := Store{DB: db, Version: 1}
	oldServer := httptest.NewServer(old.Handler())
	t.Cleanup(oldServer.Close)
	exec(t, db, Expand)
	next := Store{DB: db, Version: 2}
	newServer := httptest.NewServer(next.Handler())
	t.Cleanup(newServer.Close)
	t.Logf("v1=%s v2=%s (explicit client addresses)", oldServer.URL, newServer.URL)
	first := []Finding{{PageURL: "http://localhost:8090/", Code: "missing-content-security-policy"}}
	changed := []Finding{{PageURL: "http://localhost:8090/", Code: "missing-content-type-options"}}
	put(t, newServer.URL, first)
	// A still-running old version updates only the old column.
	put(t, oldServer.URL, changed)
	if got := get(t, newServer.URL); !reflect.DeepEqual(got.Findings, changed) {
		t.Fatal("v2 did not read authoritative old field")
	}
	oldServer.Close()
	exec(t, db, Backfill)
	readingNew := Store{DB: db, Version: 2, ReadNew: true}
	r, err := readingNew.Read(ctx, "a-42")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.Findings, changed) {
		t.Fatal("backfill retained stale non-null data")
	}
	if err = readingNew.Write(ctx, Result{AuditID: "a-42", Findings: first}); err != nil {
		t.Fatal(err)
	}
	rollback := httptest.NewServer(old.Handler())
	defer rollback.Close()
	if got := get(t, rollback.URL); !reflect.DeepEqual(got.Findings, first) {
		t.Fatal("v1 rollback cannot read v2 write")
	}
}
func TestContractClosesRollbackWindow(t *testing.T) {
	db := database(t)
	ctx := context.Background()
	old := Store{DB: db, Version: 1}
	if err := old.Write(ctx, Result{AuditID: "a-42", Findings: []Finding{}}); err != nil {
		t.Fatal(err)
	}
	exec(t, db, Expand)
	exec(t, db, Backfill)
	exec(t, db, `ALTER TABLE results ALTER COLUMN findings DROP NOT NULL`)
	finalWriter := Store{DB: db, Version: 3, ReadNew: true}
	if err := finalWriter.Write(ctx, Result{AuditID: "a-43", Findings: []Finding{}}); err != nil {
		t.Fatal(err)
	}
	exec(t, db, Contract)
	if _, err := old.Read(ctx, "a-42"); err == nil {
		t.Fatal("old binary unexpectedly reads removed column")
	}
	newReader := Store{DB: db, Version: 3, ReadNew: true}
	if _, err := newReader.Read(ctx, "a-42"); err != nil {
		t.Fatal(err)
	}
}
