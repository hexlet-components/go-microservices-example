package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	demo "hexlet.local/lifecycle"
)

func main() {
	var config demo.Config
	flag.StringVar(&config.Role, "role", "checks", "checks or reports")
	flag.StringVar(&config.Schema, "schema", "lifecycle_demo", "dedicated schema")
	flag.StringVar(&config.Topic, "topic", "lifecycle-jobs", "input topic")
	flag.StringVar(&config.OutputTopic, "output-topic", "lifecycle-checked", "output topic")
	flag.StringVar(&config.Group, "group", "lifecycle-checks", "consumer group")
	flag.StringVar(&config.SiteOrigin, "site-origin", "http://127.0.0.1:8193", "prepared fixture origin")
	flag.StringVar(&config.Listen, "listen", "127.0.0.1:8173", "health HTTP address")
	flag.DurationVar(&config.Drain, "drain", 3*time.Second, "maximum shutdown duration")
	initialize := flag.Bool("init", false, "initialize dedicated schema and topics")
	flag.Parse()
	config.DSN = os.Getenv("DATABASE_URL")
	config.Brokers = os.Getenv("KAFKA_BROKERS")
	if config.DSN == "" || config.Brokers == "" {
		log.Fatal("DATABASE_URL and KAFKA_BROKERS are required")
	}
	stop, release := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer release()
	if *initialize {
		ctx, cancel := context.WithTimeout(stop, 15*time.Second)
		defer cancel()
		if err := demo.Initialize(ctx, config); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := demo.Run(stop, config); err != nil {
		log.Print(err)
		os.Exit(2)
	}
}
