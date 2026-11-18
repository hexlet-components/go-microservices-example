package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	audit "hexlet.local/auditdemo"
)

func listen(defaultAddress string) string {
	if value := os.Getenv("LISTEN_ADDR"); value != "" {
		return value
	}
	return defaultAddress
}
func main() {
	site := flag.Bool("site", false, "serve the local fixture")
	token := flag.Bool("token", false, "print a demo token for user 1")
	role := flag.String("role", "api", "api or checks")
	flag.Parse()
	if *site {
		serve(listen("127.0.0.1:8090"), audit.Fixture())
		return
	}
	secret := os.Getenv("JWT_SECRET")
	if (*role == "api" || *token) && len(secret) < 32 {
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
	if *role != "api" && *role != "checks" {
		log.Fatal("role must be api or checks")
	}
	origin := os.Getenv("SITE_ORIGIN")
	checker, err := audit.NewChecker(origin)
	if err != nil {
		log.Fatal("SITE_ORIGIN must contain the fixture origin")
	}
	originURL, err := url.Parse(origin)
	if err != nil {
		log.Fatal(err)
	}
	checksURL := os.Getenv("CHECKS_URL")
	if *role == "api" {
		u, err := url.Parse(checksURL)
		if err != nil || u.Host == "" || u.User != nil || u.Scheme != "http" {
			log.Fatal("CHECKS_URL must contain an HTTP origin")
		}
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
	if *role == "checks" {
		store := audit.CheckStore{DB: db}
		if err := store.Migrate(ctx); err != nil {
			log.Fatal(err)
		}
		serve(listen("127.0.0.1:8081"), audit.ChecksAPI(audit.CheckService{Store: store, Checker: checker}))
		return
	}
	store := audit.Store{DB: db}
	if err := store.Migrate(ctx); err != nil {
		log.Fatal(err)
	}
	client := audit.HTTPChecks{BaseURL: checksURL, Client: &http.Client{Timeout: 3 * time.Second}}
	service := audit.Service{Store: store, Checks: client, Origin: originURL}
	serve(listen("127.0.0.1:8080"), audit.API(service, secret))
}
func serve(address string, handler http.Handler) {
	server := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 2 * time.Second}
	log.Printf("ready on %s", address)
	log.Fatal(server.ListenAndServe())
}
