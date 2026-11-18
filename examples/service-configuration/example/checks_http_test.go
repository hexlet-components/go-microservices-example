package auditdemo

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type fixedChecks struct {
	result Result
	state  CheckState
	err    error
}

func (c fixedChecks) Start(context.Context, Audit) error                { return c.err }
func (c fixedChecks) Result(context.Context, string) (Result, error)    { return c.result, c.err }
func (c fixedChecks) State(context.Context, string) (CheckState, error) { return c.state, c.err }

func TestHTTPResultContract(t *testing.T) {
	for _, test := range []struct {
		name   string
		source error
	}{
		{"unknown", ErrNotFound}, {"running", ErrNotReady}, {"dependency", errors.New("database unavailable")},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(ChecksAPI(fixedChecks{err: test.source}))
			defer server.Close()
			client := HTTPChecks{BaseURL: server.URL, Client: server.Client()}
			_, err := client.Result(context.Background(), "audit-1")
			if err == nil {
				t.Fatal("error must not become an empty result")
			}
			if test.source == ErrNotFound || test.source == ErrNotReady {
				if !errors.Is(err, test.source) {
					t.Fatalf("got %v, want %v", err, test.source)
				}
			}
		})
	}
}

func TestHTTPResultCancellation(t *testing.T) {
	entered := make(chan struct{})
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(done)
	}))
	defer server.Close()
	client := HTTPChecks{BaseURL: server.URL, Client: server.Client()}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := client.Result(ctx, "audit-1"); result <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not reach server")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not cancel")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("server did not observe cancellation")
	}
}

func TestHTTPIntegration(t *testing.T) {
	requestDSN := os.Getenv("TEST_REQUESTS_DATABASE_URL")
	checkDSN := os.Getenv("TEST_CHECKS_DATABASE_URL")
	if requestDSN == "" || checkDSN == "" {
		t.Skip("set both TEST_REQUESTS_DATABASE_URL and TEST_CHECKS_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	open := func(dsn string) *sql.DB {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		if err := db.PingContext(ctx); err != nil {
			t.Fatal(err)
		}
		return db
	}
	requestDB := open(requestDSN)
	checkDB := open(checkDSN)
	requests := Store{DB: requestDB}
	checks := CheckStore{DB: checkDB}
	if err := requests.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := checks.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	fixture := httptest.NewServer(Fixture())
	defer fixture.Close()
	checker, err := NewChecker(fixture.URL)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(ChecksAPI(CheckService{Store: checks, Checker: checker}))
	defer server.Close()
	client := HTTPChecks{BaseURL: server.URL, Client: server.Client()}
	origin, err := url.Parse(fixture.URL)
	if err != nil {
		t.Fatal(err)
	}
	service := Service{Store: requests, Checks: client, Origin: origin}
	key, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(ctx, 1, key, fixture.URL+"/")
	if err != nil {
		t.Fatal(err)
	}
	report, err := service.Report(ctx, 1, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Ready || len(report.Findings) != 2 {
		t.Fatalf("unexpected report %#v", report)
	}
	if _, err := service.Report(ctx, 2, created.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign report accessible: %v", err)
	}
	result, err := client.Result(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := service.Create(ctx, 1, key, fixture.URL+"/")
	if err != nil {
		t.Fatal(err)
	}
	again, err := client.Result(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if repeated.ID != created.ID || again.ID != result.ID {
		t.Fatal("repeat replaced identity")
	}
	if _, err := service.Create(ctx, 1, key, fixture.URL+"/about"); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed request must conflict: %v", err)
	}
	pendingID, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	if err := checks.Register(ctx, Audit{ID: pendingID, URL: fixture.URL + "/"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Result(ctx, pendingID); !errors.Is(err, ErrNotReady) {
		t.Fatalf("pending must differ from missing: %v", err)
	}
	if state, err := client.State(ctx, pendingID); err != nil || state != CheckRunning {
		t.Fatalf("state=%s err=%v", state, err)
	}
	if _, err := client.Result(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown audit: %v", err)
	}
	if _, err := requestDB.ExecContext(ctx, "SELECT * FROM results"); err == nil {
		t.Fatal("checks table leaked into requests database")
	}
	if _, err := checkDB.ExecContext(ctx, "SELECT * FROM audits"); err == nil {
		t.Fatal("requests table leaked into checks database")
	}
}
