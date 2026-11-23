package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/twmb/franz-go/pkg/kgo"
	demo "hexlet.local/auditdemo"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	role := flag.String("role", "api", "api, checks, reports, site or token")
	flag.Parse()
	if err := run(*role); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("stopped", "error", err)
		os.Exit(1)
	}
}

func run(role string) error {
	if role == "token" {
		token, err := demo.Token(os.Getenv("JWT_SECRET"), 1)
		if err == nil {
			fmt.Println(token)
		}
		return err
	}
	if role == "site" {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `<title>Audit fixture</title><p>No security headers</p>`)
		})
		return http.ListenAndServe("127.0.0.1:8090", mux)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	store := demo.Store{DB: db}
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = store.Migrate(startup, role)
	cancel()
	if err != nil {
		return err
	}
	options := []kgo.Opt{kgo.SeedBrokers("localhost:19092"), kgo.RequiredAcks(kgo.AllISRAcks())}
	if role == "checks" || role == "reports" {
		topic := "audit-created"
		if role == "reports" {
			topic = "audit-checked"
		}
		group := os.Getenv("CONSUMER_GROUP")
		if group == "" {
			group = role + "-v1"
		}
		options = append(options, kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
	}
	client, err := kgo.NewClient(options...)
	if err != nil {
		return err
	}
	defer client.Close()
	checker, err := demo.NewChecker("http://localhost:8090")
	if err != nil {
		return err
	}
	if role == "checks" {
		return demo.Consume(ctx, client, func(ctx context.Context, r *kgo.Record) error {
			var e demo.AuditCreated
			if err := json.Unmarshal(r.Value, &e); err != nil {
				return err
			}
			if e.EventID == "" || e.AuditID == "" {
				return demo.ErrInvalid
			}
			findings, err := checker.Check(ctx, e.URL)
			if err != nil {
				return err
			}
			result, err := store.SaveCheck(ctx, e.AuditID, findings)
			if err != nil {
				return err
			}
			return demo.Publish(ctx, client, "audit-checked", e.AuditID, result)
		})
	}
	if role == "reports" {
		errorsCh := make(chan error, 1)
		go func() {
			errorsCh <- demo.Consume(ctx, client, func(ctx context.Context, r *kgo.Record) error {
				var e demo.AuditChecked
				if err := json.Unmarshal(r.Value, &e); err != nil {
					return err
				}
				if e.EventID == "" || e.ResultID == "" || e.AuditID == "" {
					return demo.ErrInvalid
				}
				return store.SaveReport(ctx, e)
			})
		}()
		mux := http.NewServeMux()
		mux.HandleFunc("GET /reports/{id}", func(w http.ResponseWriter, r *http.Request) {
			report, err := store.Report(r.Context(), r.PathValue("id"))
			if err != nil {
				http.Error(w, "reports unavailable", 503)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(report)
		})
		return serve(ctx, "127.0.0.1:8082", mux, errorsCh)
	}
	if role != "api" {
		return demo.ErrInvalid
	}
	secret := os.Getenv("JWT_SECRET")
	if len(secret) < 32 {
		return fmt.Errorf("JWT_SECRET needs at least 32 bytes")
	}
	s := demo.Service{Store: store, Checker: checker, Kafka: client, ReportsURL: "http://localhost:8082", HTTP: &http.Client{Timeout: time.Second}}
	return serve(ctx, "127.0.0.1:8080", demo.API(s, secret), nil)
}

func serve(ctx context.Context, address string, handler http.Handler, workerErrors <-chan error) error {
	server := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: time.Second}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	var err error
	select {
	case err = <-serverErrors:
	case err = <-workerErrors:
	case <-ctx.Done():
		err = ctx.Err()
	}
	drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdownErr := server.Shutdown(drain)
	if err != nil {
		return err
	}
	return shutdownErr
}
