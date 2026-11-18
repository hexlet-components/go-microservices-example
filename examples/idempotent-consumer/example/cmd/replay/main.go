package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	demo "hexlet.local/idempotent"
)

func main() {
	schema := flag.String("schema", "idempotent_demo", "dedicated demo schema")
	eventID := flag.String("event", "e-42", "stable event identifier")
	fail := flag.Bool("fail-after-marker", false, "fail before writing the report")
	flag.Parse()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := demo.Open(ctx, dsn, *schema)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	store := demo.Store{DB: db}
	if *fail {
		store.BeforeReport = func() error { return fmt.Errorf("injected failure after marker") }
	}
	event := demo.AuditChecked{
		EventID: *eventID, AuditID: "a-42", ResultID: "r-42",
		Findings: []demo.Finding{{PageURL: "http://localhost:8090/about", Code: "missing-title"}},
	}
	applied, err := store.Apply(ctx, "reports-v1", event)
	if err != nil {
		log.Fatal(err)
	}
	applications, markers, err := store.Statistics(ctx, event.AuditID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("event=%s applied=%t applications=%d markers=%d\n", event.EventID, applied, applications, markers)
}
