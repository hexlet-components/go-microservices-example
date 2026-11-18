package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	demo "hexlet.local/tracedemo"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	dsn, brokers, endpoint := os.Getenv("DATABASE_URL"), strings.Split(os.Getenv("KAFKA_BROKERS"), ","), os.Getenv("OTLP_ENDPOINT")
	if dsn == "" || brokers[0] == "" || endpoint == "" {
		log.Fatal("DATABASE_URL, KAFKA_BROKERS and OTLP_ENDPOINT are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	store := demo.SQLStore{DB: db}
	if err := store.Migrate(ctx); err != nil {
		log.Fatal(err)
	}
	sampler := sdktrace.AlwaysSample()
	if os.Getenv("SAMPLE_TRACES") == "off" {
		sampler = sdktrace.NeverSample()
	}
	makeProvider := func(name string) *sdktrace.TracerProvider {
		exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
		if err != nil {
			log.Fatal(err)
		}
		return demo.Provider(name, exporter, sampler, false)
	}
	providers := demo.Providers{API: makeProvider("api"), Checks: makeProvider("checks"), Sender: makeProvider("outbox"), Reports: makeProvider("reports")}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := providers.Shutdown(ctx); err != nil {
			log.Print(err)
		}
	}()
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		log.Fatal(err)
	}
	auditID := hex.EncodeToString(id[:])
	topic := "audit-trace-" + auditID
	if err := demo.CreateTopic(ctx, brokers, topic); err != nil {
		log.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		demo.DeleteTopic(ctx, brokers, topic)
	}()
	outcome, err := demo.Run(ctx, store, providers, brokers, topic, auditID, "prepared-local-service-token")
	if err != nil {
		log.Fatal(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(outcome); err != nil {
		log.Fatal(err)
	}
}
