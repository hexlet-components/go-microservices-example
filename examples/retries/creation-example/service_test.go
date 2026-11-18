package auditdemo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func testService(t *testing.T) (Service, string) {
	t.Helper()
	dsn := os.Getenv("AUDIT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("AUDIT_TEST_DATABASE_URL is required for PostgreSQL integration")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	id, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	schemaName := "audit_test_" + id
	// Each test owns a fresh schema. Shared tables are never cleared or modified.
	if _, err := admin.Exec("CREATE SCHEMA " + schemaName); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schemaName)
	parsed.RawQuery = query.Encode()
	db, err := sql.Open("pgx", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	store := Store{DB: db}
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	site := httptest.NewServer(Fixture())
	t.Cleanup(site.Close)
	checker, err := NewChecker(site.URL)
	if err != nil {
		t.Fatal(err)
	}
	return Service{Store: store, Checker: checker}, site.URL + "/"
}

func TestCreateAndReadReport(t *testing.T) {
	service, target := testService(t)
	ctx := context.Background()
	audit, err := service.Create(ctx, 1, "one", target)
	if err != nil {
		t.Fatal(err)
	}
	report, err := service.Report(ctx, 1, audit.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Ready || len(report.Findings) != 2 {
		t.Fatalf("report=%+v", report)
	}
	if _, err := service.Report(ctx, 2, audit.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("owner access error=%v", err)
	}
	again, err := service.Create(ctx, 1, "one", target)
	if err != nil || again.ID != audit.ID {
		t.Fatalf("again=%+v err=%v", again, err)
	}
	if _, err := service.Create(ctx, 1, "one", target+"about"); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict error=%v", err)
	}
}

func TestConcurrentIdempotency(t *testing.T) {
	service, target := testService(t)
	const attempts = 6
	results := make(chan Audit, attempts)
	failures := make(chan error, attempts)
	var group sync.WaitGroup
	for i := 0; i < attempts; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			audit, err := service.Create(context.Background(), 1, "same", target)
			if err != nil {
				failures <- err
				return
			}
			results <- audit
		}()
	}
	group.Wait()
	close(results)
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	id := ""
	for a := range results {
		if id != "" && a.ID != id {
			t.Fatal("different audit IDs")
		}
		id = a.ID
	}
	for _, table := range []string{"audits", "results", "reports"} {
		var count int
		if err := service.Store.DB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("%s count=%d", table, count)
		}
	}
	first, err := service.Store.Result(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Create(context.Background(), 1, "same", target); err != nil {
		t.Fatal(err)
	}
	second, err := service.Store.Result(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatal("result ID changed")
	}
}

func TestResultAndReportRollbackTogether(t *testing.T) {
	service, target := testService(t)
	ctx := context.Background()
	audit, err := service.Store.Create(ctx, 1, "rollback", target)
	if err != nil {
		t.Fatal(err)
	}
	// The trigger belongs to this test's fresh schema only.
	_, err = service.Store.DB.Exec(`CREATE FUNCTION fail_report() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test failure'; END $$;
 CREATE TRIGGER test_fail_report BEFORE INSERT ON reports FOR EACH ROW EXECUTE FUNCTION fail_report()`)
	if err != nil {
		t.Fatal(err)
	}
	err = service.Store.SaveResult(ctx, Result{ID: "result", AuditID: audit.ID, Findings: []Finding{}})
	if err == nil {
		t.Fatal("expected failed report insert")
	}
	if _, err := service.Store.Result(ctx, audit.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("result must roll back, error=%v", err)
	}
	report, err := service.Report(ctx, 1, audit.ID)
	if err != nil || report.Ready {
		t.Fatalf("report=%+v err=%v", report, err)
	}
}

func TestHTTPScenario(t *testing.T) {
	service, target := testService(t)
	secret := "local-example-secret-32-characters"
	token, err := Token(secret, 1)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API(service, secret))
	defer server.Close()
	body := strings.NewReader(fmt.Sprintf(`{"url":%q}`, target))
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/audits", body)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Idempotency-Key", "api-demo")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d", response.StatusCode)
	}
	var created struct {
		ID string `json:"audit_id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	request, _ = http.NewRequest(http.MethodGet, server.URL+"/audits/"+created.ID+"/report", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var report Report
	if err := json.NewDecoder(response.Body).Decode(&report); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !report.Ready || len(report.Findings) != 2 {
		t.Fatalf("status=%d report=%+v", response.StatusCode, report)
	}
	request, _ = http.NewRequest(http.MethodGet, server.URL+"/audits/"+created.ID+"/report", nil)
	request.Header.Set("Authorization", "Bearer bad-token")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", response.StatusCode)
	}
}
