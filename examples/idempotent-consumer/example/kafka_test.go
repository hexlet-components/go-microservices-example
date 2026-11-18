package idempotent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func kafkaTopic(t *testing.T) (*kgo.Client, string, string) {
	t.Helper()
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		t.Fatal("KAFKA_BROKERS is required for Kafka integration tests")
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	topic, group := uniqueName("dedup_topic_"), uniqueName("dedup_group_")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	create := kmsg.NewPtrCreateTopicsRequest()
	create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
	response, err := create.RequestWith(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	if err := kerr.ErrorForCode(response.Topics[0].ErrorCode); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		remove := kmsg.NewPtrDeleteTopicsRequest()
		remove.TopicNames = []string{topic}
		remove.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &topic}}
		response, err := remove.RequestWith(ctx, client)
		if err != nil {
			t.Error(err)
			return
		}
		for _, result := range response.Topics {
			if err := kerr.ErrorForCode(result.ErrorCode); err != nil {
				t.Error(err)
			}
		}
	})
	return client, topic, group
}

func kafkaConsumer(t *testing.T, topic, group string) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(os.Getenv("KAFKA_BROKERS"), ",")...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.SessionTimeout(6*time.Second),
		kgo.HeartbeatInterval(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func nextRecord(t *testing.T, ctx context.Context, client *kgo.Client) *kgo.Record {
	t.Helper()
	fetches := client.PollRecords(ctx, 1)
	if errs := fetches.Errors(); len(errs) > 0 {
		client.AllowRebalance()
		t.Fatal(errs[0].Err)
	}
	records := fetches.Records()
	if len(records) != 1 {
		client.AllowRebalance()
		t.Fatalf("got %d records, want 1", len(records))
	}
	return records[0]
}

func publishChecked(t *testing.T, ctx context.Context, client *kgo.Client, topic string) {
	t.Helper()
	event := checkedEvent()
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(event.AuditID), Value: data}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func TestRepublishedEventAtNewOffset(t *testing.T) {
	store, _ := isolatedStore(t)
	producer, topic, group := kafkaTopic(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	publishChecked(t, ctx, producer, topic)
	publishChecked(t, ctx, producer, topic)
	client := kafkaConsumer(t, topic, group)
	defer func() { client.AllowRebalance(); client.Close() }()
	for index := int64(0); index < 2; index++ {
		record := nextRecord(t, ctx, client)
		if record.Offset != index {
			t.Fatalf("offset=%d, want %d", record.Offset, index)
		}
		if err := store.HandleRecord(ctx, client, record); err != nil {
			t.Fatal(err)
		}
		client.AllowRebalance()
	}
	assertStatistics(t, store, 1, 1)
	if got := client.CommittedOffsets()[topic][0].Offset; got != 2 {
		t.Fatalf("committed offset=%d, want 2", got)
	}
}

func TestCrashAfterCommitBeforeOffset(t *testing.T) {
	if os.Getenv("DEDUPE_CRASH_CHILD") == "1" {
		runCrashChild(t)
		return
	}
	store, schema := isolatedStore(t)
	producer, topic, group := kafkaTopic(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	publishChecked(t, ctx, producer, topic)
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrashAfterCommitBeforeOffset$", "-test.timeout=30s")
	child.Env = append(os.Environ(), "DEDUPE_CRASH_CHILD=1", "DEDUPE_SCHEMA="+schema, "DEDUPE_TOPIC="+topic, "DEDUPE_GROUP="+group)
	output, err := child.CombinedOutput()
	exitError, ok := err.(*exec.ExitError)
	if !ok || exitError.ExitCode() != 23 || !strings.Contains(string(output), "db_committed_without_offset") {
		t.Fatalf("crash child: err=%v output=%s", err, output)
	}
	assertStatistics(t, store, 1, 1)
	client := kafkaConsumer(t, topic, group)
	defer func() { client.AllowRebalance(); client.Close() }()
	record := nextRecord(t, ctx, client)
	if record.Offset != 0 {
		t.Fatalf("replayed offset=%d, want 0", record.Offset)
	}
	if err := store.HandleRecord(ctx, client, record); err != nil {
		t.Fatal(err)
	}
	client.AllowRebalance()
	assertStatistics(t, store, 1, 1)
	if got := client.CommittedOffsets()[topic][0].Offset; got != 1 {
		t.Fatalf("committed offset=%d, want 1", got)
	}
	t.Log("replayed offset=0 applications=1 markers=1 committed offset=1")
}

func runCrashChild(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	db, err := Open(ctx, os.Getenv("DATABASE_URL"), os.Getenv("DEDUPE_SCHEMA"))
	if err != nil {
		t.Fatal(err)
	}
	client := kafkaConsumer(t, os.Getenv("DEDUPE_TOPIC"), os.Getenv("DEDUPE_GROUP"))
	record := nextRecord(t, ctx, client)
	var event AuditChecked
	if err := json.Unmarshal(record.Value, &event); err != nil {
		t.Fatal(err)
	}
	if _, err := (Store{DB: db}).Apply(ctx, "reports-v1", event); err != nil {
		t.Fatal(err)
	}
	fmt.Println("db_committed_without_offset")
	os.Exit(23)
}
