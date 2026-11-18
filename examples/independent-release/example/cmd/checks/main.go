package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/twmb/franz-go/pkg/kgo"
	release "hexlet.local/independentrelease"
)

var version = "v1"
var ruleVersion = "2026-11"

func main() {
	address := flag.String("grpc", "127.0.0.1:9090", "RPC address")
	httpAddress := flag.String("http", "127.0.0.1:8093", "fixture control address")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	client, err := kgo.NewClient(kgo.SeedBrokers(release.Env("KAFKA_BROKER", "127.0.0.1:19095")), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	store := release.Store{DB: db, Version: version, RuleVersion: ruleVersion}
	if err := release.RunChecks(ctx, store, client, release.Env("EVENT_TOPIC", "release.audit-checked"), *address, *httpAddress); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
