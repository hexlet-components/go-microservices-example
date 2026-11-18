package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	release "hexlet.local/independentrelease"
)

func main() {
	address := flag.String("http", "127.0.0.1:8080", "API address")
	checks := flag.String("checks", "127.0.0.1:9090", "checks RPC address")
	reports := flag.String("reports", "http://127.0.0.1:8082", "reports URL")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := release.RunAPI(ctx, *checks, *reports, *address); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
