package failures

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestKafkaDeadLetterAndSourceProgress(t *testing.T) {
	broker := os.Getenv("KAFKA_BROKERS")
	if broker == "" {
		t.Skip("KAFKA_BROKERS is required for the real Kafka integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	topic := fmt.Sprintf("gm.failures.%d.checked", time.Now().UnixNano())
	admin, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	request := kmsg.NewPtrCreateTopicsRequest()
	for _, name := range []string{topic, topic + "-dlt"} {
		request.Topics = append(request.Topics, kmsg.CreateTopicsRequestTopic{Topic: name, NumPartitions: 1, ReplicationFactor: 1})
	}
	response, err := admin.Request(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range response.(*kmsg.CreateTopicsResponse).Topics {
		if row.ErrorCode != 0 {
			t.Fatalf("create %s error=%d", row.Topic, row.ErrorCode)
		}
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic), kgo.ConsumerGroup(topic+".group"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.CloseAllowingRebalance()
	source := &kgo.Record{Topic: topic, Value: []byte(`{"event_id":"e-bad"}`)}
	if err := admin.ProduceSync(ctx, source).FirstErr(); err != nil {
		t.Fatal(err)
	}
	fetches := consumer.PollRecords(ctx, 1)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatal(errs[0].Err)
	}
	records := fetches.Records()
	if len(records) != 1 {
		t.Fatal("source record not received")
	}
	record := records[0]
	processor := Processor{MaxAttempts: 3, Publish: KafkaPublish(admin), Commit: func(context.Context, *kgo.Record) error { return errors.New("interruption after DLT ack") }}
	if err := processor.Process(ctx, record); err == nil {
		t.Fatal("expected interruption")
	}
	if offset, ok := consumer.CommittedOffsets()[topic][0]; ok && offset.Offset > record.Offset {
		t.Fatalf("source advanced before its commit: %+v", offset)
	}
	processor.Commit = KafkaCommit(consumer)
	if err := processor.Process(ctx, record); err != nil {
		t.Fatal(err)
	}
	offset, ok := consumer.CommittedOffsets()[topic][0]
	if !ok || offset.Offset != record.Offset+1 {
		t.Fatalf("committed=%+v ok=%v", offset, ok)
	}
	consumer.AllowRebalance()
	reader, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic + "-dlt": {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var letters []DeadLetter
	for len(letters) < 2 {
		received := reader.PollRecords(ctx, 2-len(letters))
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		if errs := received.Errors(); len(errs) > 0 {
			t.Fatal(errs[0].Err)
		}
		for _, r := range received.Records() {
			var letter DeadLetter
			if err := json.Unmarshal(r.Value, &letter); err != nil {
				t.Fatal(err)
			}
			letters = append(letters, letter)
		}
	}
	if letters[0].EventID != letters[1].EventID || letters[0].SourceOffset != record.Offset || letters[0].SourceTopic != topic || letters[0].SourceEventID != "e-bad" {
		t.Fatalf("letters=%+v", letters)
	}
	t.Logf("received %d DLT records with the same identity; source next offset=%d", len(letters), offset.Offset)
}
