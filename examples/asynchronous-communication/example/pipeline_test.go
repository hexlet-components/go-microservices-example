package auditdemo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func isolated(t *testing.T, env, role string) Store {
	t.Helper()
	dsn := os.Getenv(env)
	if dsn == "" {
		t.Skip("set " + env + " for integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	suffix, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	name := "pipeline_" + suffix
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
	store := Store{DB: db}
	if err = store.Migrate(ctx, role); err != nil {
		t.Fatal(err)
	}
	return store
}

func TestRealPipeline(t *testing.T) {
	broker := os.Getenv("AUDIT_TEST_BROKERS")
	if broker == "" {
		t.Skip("set AUDIT_TEST_BROKERS for Kafka integration")
	}
	apiStore := isolated(t, "AUDIT_TEST_REQUESTS_DSN", "api")
	checksStore := isolated(t, "AUDIT_TEST_CHECKS_DSN", "checks")
	reportsStore := isolated(t, "AUDIT_TEST_REPORTS_DSN", "reports")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	producer, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	suffix, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	created := "created-" + suffix
	checked := "checked-" + suffix
	request := kmsg.NewPtrCreateTopicsRequest()
	for _, topic := range []string{created, checked} {
		entry := kmsg.NewCreateTopicsRequestTopic()
		entry.Topic = topic
		entry.NumPartitions = 1
		entry.ReplicationFactor = 1
		request.Topics = append(request.Topics, entry)
	}
	response, err := request.RequestWith(ctx, producer)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range response.Topics {
		if entry.ErrorCode != 0 {
			t.Fatalf("create topic error %d", entry.ErrorCode)
		}
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		request := kmsg.NewPtrDeleteTopicsRequest()
		request.TopicNames = []string{created, checked}
		request.RequestWith(cleanup, producer)
	}()
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<title>Fixture</title><p>No security headers</p>`))
	}))
	defer fixture.Close()
	checker, err := NewChecker(fixture.URL)
	if err != nil {
		t.Fatal(err)
	}
	reportServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		report, err := reportsStore.Report(r.Context(), r.URL.Path[len("/reports/"):])
		if err != nil {
			http.Error(w, "unavailable", 503)
			return
		}
		json.NewEncoder(w).Encode(report)
	}))
	defer reportServer.Close()
	service := Service{Store: apiStore, Checker: checker, Kafka: producer, CreatedTopic: created, ReportsURL: reportServer.URL, HTTP: reportServer.Client()}
	audit, err := service.Create(ctx, 1, "pipeline-key", fixture.URL+"/")
	if err != nil {
		t.Fatal(err)
	}
	before, err := service.Report(ctx, 1, audit.ID)
	if err != nil {
		t.Fatal(err)
	}
	if before.Ready {
		t.Fatal("report ready before processing")
	}
	if _, err = service.Report(ctx, 2, audit.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another owner access: %v", err)
	}
	worker := func(topic, group string) *kgo.Client {
		t.Helper()
		client, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	checks := worker(created, "checks-"+suffix)
	reports := worker(checked, "reports-"+suffix)
	runCtx, stopWorkers := context.WithCancel(ctx)
	done := make(chan error, 2)
	go func() {
		done <- Consume(runCtx, checks, func(ctx context.Context, r *kgo.Record) error {
			var e AuditCreated
			if err := json.Unmarshal(r.Value, &e); err != nil {
				return err
			}
			findings, err := checker.Check(ctx, e.URL)
			if err != nil {
				return err
			}
			result, err := checksStore.SaveCheck(ctx, e.AuditID, findings)
			if err != nil {
				return err
			}
			return Publish(ctx, producer, checked, e.AuditID, result)
		})
	}()
	go func() {
		done <- Consume(runCtx, reports, func(ctx context.Context, r *kgo.Record) error {
			var e AuditChecked
			if err := json.Unmarshal(r.Value, &e); err != nil {
				return err
			}
			return reportsStore.SaveReport(ctx, e)
		})
	}()
	defer func() {
		stopWorkers()
		for i := 0; i < 2; i++ {
			err := <-done
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Error(err)
			}
		}
		checks.Close()
		reports.Close()
	}()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		report, err := service.Report(ctx, 1, audit.ID)
		if err != nil {
			t.Fatal(err)
		}
		if report.Ready {
			if len(report.Findings) != 2 {
				t.Fatalf("findings=%+v", report.Findings)
			}
			codes := map[string]bool{}
			for _, f := range report.Findings {
				codes[f.Code] = true
			}
			if !codes["missing-content-security-policy"] || !codes["missing-content-type-options"] {
				t.Fatal(report.Findings)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("report not ready before deadline")
		case <-ticker.C:
		}
	}
}
