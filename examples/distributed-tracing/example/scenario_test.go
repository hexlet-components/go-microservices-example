package tracedemo

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func id(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b[:])
}
func telemetry(t *testing.T, sample bool) (Providers, map[string]*tracetest.InMemoryExporter) {
	t.Helper()
	exporters := map[string]*tracetest.InMemoryExporter{}
	sampler := sdktrace.AlwaysSample()
	if !sample {
		sampler = sdktrace.NeverSample()
	}
	provider := func(name string) *sdktrace.TracerProvider {
		e := tracetest.NewInMemoryExporter()
		exporters[name] = e
		return Provider(name, e, sampler, true)
	}
	p := Providers{API: provider("api"), Checks: provider("checks"), Sender: provider("outbox"), Reports: provider("reports")}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		p.Shutdown(ctx)
	})
	return p, exporters
}
func topic(t *testing.T) ([]string, string) {
	t.Helper()
	value := os.Getenv("KAFKA_BROKERS")
	if value == "" {
		t.Skip("KAFKA_BROKERS is required")
	}
	brokers := strings.Split(value, ",")
	topic := "trace-test-" + id(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := CreateTopic(ctx, brokers, topic); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		DeleteTopic(ctx, brokers, topic)
	})
	return brokers, topic
}
func findSpan(t *testing.T, e *tracetest.InMemoryExporter, kind trace.SpanKind, name string) tracetest.SpanStub {
	t.Helper()
	for _, span := range e.GetSpans() {
		if span.SpanKind == kind && (name == "" || span.Name == name) {
			return span
		}
	}
	t.Fatalf("span kind=%s name=%s missing from %#v", kind, name, e.GetSpans())
	return tracetest.SpanStub{}
}

func assertChain(t *testing.T, store Outbox) {
	t.Helper()
	brokers, topic := topic(t)
	p, exporters := telemetry(t, true)
	auditID := id(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	outcome, err := Run(ctx, store, p, brokers, topic, auditID, demoToken)
	if err != nil || !outcome.Ready {
		t.Fatalf("outcome=%+v error=%v", outcome, err)
	}
	root := findSpan(t, exporters["api"], trace.SpanKindInternal, "audit.accept")
	processing := findSpan(t, exporters["checks"], trace.SpanKindInternal, "checks.complete")
	client := findSpan(t, exporters["checks"], trace.SpanKindClient, "")
	server := findSpan(t, exporters["api"], trace.SpanKindServer, "")
	sender := findSpan(t, exporters["outbox"], trace.SpanKindProducer, "outbox.publish")
	reports := findSpan(t, exporters["reports"], trace.SpanKindConsumer, "reports.apply")
	for _, span := range []tracetest.SpanStub{root, processing, client, server, sender, reports} {
		if span.SpanContext.TraceID().String() != outcome.TraceID {
			t.Fatalf("trace was split at %s", span.Name)
		}
	}
	for _, pair := range [][2]tracetest.SpanStub{{root, processing}, {processing, client}, {client, server}, {processing, sender}, {sender, reports}} {
		if pair[1].Parent.SpanID() != pair[0].SpanContext.SpanID() {
			t.Fatalf("%s lost parent %s", pair[1].Name, pair[0].Name)
		}
	}
	row, err := store.Load(ctx, outcome.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(row.Carrier["traceparent"], outcome.TraceID) {
		t.Fatal("outbox lost durable trace carrier")
	}
	if outcome.AuditID == outcome.TraceID {
		t.Fatal("business ID used as trace ID")
	}
}
func TestTraceSurvivesRPCOutboxAndKafka(t *testing.T) { assertChain(t, &MemoryStore{}) }

func TestTraceWithDurableSQLCarrier(t *testing.T) {
	dsn := os.Getenv("TRACE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TRACE_TEST_DATABASE_URL is required")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "trace_test_" + id(t)
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	db, err := sql.Open("pgx", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(); admin.Exec("DROP SCHEMA " + schema + " CASCADE"); admin.Close() })
	store := SQLStore{DB: db}
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertChain(t, store)
	var count int
	if err := db.QueryRow("SELECT count(*) FROM trace_outbox WHERE sent_at IS NOT NULL").Scan(&count); err != nil || count != 1 {
		t.Fatalf("marked=%d error=%v", count, err)
	}
}

func TestSamplingDoesNotStopTheAudit(t *testing.T) {
	brokers, topic := topic(t)
	p, exporters := telemetry(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := Run(ctx, &MemoryStore{}, p, brokers, topic, id(t), demoToken)
	if err != nil || !result.Ready || len(result.TraceID) != 32 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	for name, exporter := range exporters {
		if len(exporter.GetSpans()) != 0 {
			t.Fatalf("unsampled spans exported by %s", name)
		}
	}
}

func TestDiagnosticSpanExistsBeforeAuthentication(t *testing.T) {
	p, exporters := telemetry(t, true)
	store := &MemoryStore{}
	var log bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&log, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	outcome, err := Run(ctx, store, p, []string{"127.0.0.1:1"}, "unused", id(t), "rejected-test-token")
	if status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
	span := findSpan(t, exporters["api"], trace.SpanKindServer, "")
	if span.SpanContext.TraceID().String() != outcome.TraceID {
		t.Fatal("rejection has no caller trace")
	}
	if len(store.Events) != 0 {
		t.Fatal("rejected RPC wrote outbox")
	}
	if !strings.Contains(log.String(), "rpc_started") || !strings.Contains(log.String(), outcome.TraceID) {
		t.Fatal("rejected call is missing diagnostics")
	}
	if strings.Contains(log.String(), "rejected-test-token") || strings.Contains(log.String(), demoToken) {
		t.Fatal("token leaked into log")
	}
}
