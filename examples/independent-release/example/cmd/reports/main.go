package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	release "hexlet.local/independentrelease"
)

func main() {
	address := flag.String("http", "127.0.0.1:8082", "reports address")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	topic := release.Env("EVENT_TOPIC", "release.audit-checked")
	client, err := kgo.NewClient(kgo.SeedBrokers(release.Env("KAFKA_BROKER", "127.0.0.1:19095")), kgo.ConsumeTopics(topic), kgo.ConsumerGroup(release.Env("CONSUMER_GROUP", "release-reports-v1")), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
	if err != nil {
		log.Fatal(err)
	}
	defer client.CloseAllowingRebalance()
	setup, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = release.PrepareTopic(setup, client, topic)
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	if err := release.RunReports(ctx, client, *address); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
