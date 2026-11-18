package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/twmb/franz-go/pkg/kgo"
	audit "hexlet.local/auditdemo"
)

func main() {
	mode := flag.String("mode", "pending", "create, checked, sender or pending")
	role := flag.String("role", "api", "api or checks")
	key := flag.String("key", "example-1", "idempotency key")
	target := flag.String("url", "http://127.0.0.1:8090/", "audit URL")
	auditID := flag.String("audit-id", "", "audit ID for checked")
	findings := flag.String("findings", "[]", "computed findings as JSON")
	crash := flag.Bool("stop-after-ack", false, "stop before marking the first acknowledged event")
	flag.Parse()
	if *role != "api" && *role != "checks" {
		log.Fatal("role must be api or checks")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	store := audit.Store{DB: db}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}
	if err := store.Migrate(ctx, *role); err != nil {
		log.Fatal(err)
	}
	cancel()
	topic := os.Getenv("OUTBOX_TOPIC")
	if topic == "" {
		if *role == "api" {
			topic = "audit-created"
		} else {
			topic = "audit-checked"
		}
	}
	switch *mode {
	case "create":
		if *role != "api" {
			log.Fatal("create requires role api")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		event, err := store.Create(ctx, 1, *key, *target, topic)
		if err != nil {
			log.Fatal(err)
		}
		printJSON(event)
	case "checked":
		if *role != "checks" {
			log.Fatal("checked requires role checks")
		}
		var values []audit.Finding
		if err := json.Unmarshal([]byte(*findings), &values); err != nil {
			log.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		event, err := store.SaveCheck(ctx, *auditID, values, topic)
		if err != nil {
			log.Fatal(err)
		}
		printJSON(event)
	case "pending":
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		count, err := store.Pending(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(count)
	case "sender":
		brokers := os.Getenv("KAFKA_BROKERS")
		if brokers == "" {
			log.Fatal("KAFKA_BROKERS is required")
		}
		client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.RequiredAcks(kgo.AllISRAcks()))
		if err != nil {
			log.Fatal(err)
		}
		defer client.Close()
		sender := audit.Sender{Store: store, Publisher: audit.KafkaPublisher{Client: client}}
		if *crash {
			sender.AfterAck = func(event audit.Envelope) error {
				log.Printf("ack event_id=%s; stopping before mark", event.EventID)
				return audit.ErrStopAfterAck
			}
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := sender.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Print(err)
			return
		}
	default:
		log.Fatal("unknown mode")
	}
}
func printJSON(value any) {
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		log.Fatal(err)
	}
}
