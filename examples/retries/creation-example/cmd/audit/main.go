package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	audit "hexlet.local/auditdemo"
)

func main() {
	site := flag.Bool("site", false, "serve the local fixture")
	token := flag.Bool("token", false, "print a demo token for user 1")
	flag.Parse()
	if *site {
		log.Fatal((&http.Server{Addr: "127.0.0.1:8090", Handler: audit.Fixture(), ReadHeaderTimeout: 2 * time.Second}).ListenAndServe())
	}
	secret := os.Getenv("JWT_SECRET")
	if len(secret) < 32 {
		log.Fatal("JWT_SECRET must contain at least 32 bytes")
	}
	if *token {
		value, err := audit.Token(secret, 1)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(value)
		return
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}
	store := audit.Store{DB: db}
	if err := store.Migrate(ctx); err != nil {
		log.Fatal(err)
	}
	checker, err := audit.NewChecker("http://127.0.0.1:8090")
	if err != nil {
		log.Fatal(err)
	}
	service := audit.Service{Store: store, Checker: checker}
	server := &http.Server{Addr: "127.0.0.1:8080", Handler: audit.API(service, secret), ReadHeaderTimeout: 2 * time.Second}
	log.Print("audit API ready on 127.0.0.1:8080")
	log.Fatal(server.ListenAndServe())
}
