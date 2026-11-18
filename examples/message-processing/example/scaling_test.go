package auditdemo

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestTwoMembersAndResume(t *testing.T) {
	broker := os.Getenv("AUDIT_TEST_BROKERS")
	if broker == "" {
		t.Skip("set AUDIT_TEST_BROKERS for integration")
	}
	store := isolated(t, "AUDIT_TEST_CHECKS_DSN", "checks")
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	suffix, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	topic := "scaling-" + suffix
	group := "scaling-group-" + suffix
	producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	entry := kmsg.NewCreateTopicsRequestTopic()
	entry.Topic = topic
	entry.NumPartitions = 3
	entry.ReplicationFactor = 1
	req.Topics = []kmsg.CreateTopicsRequestTopic{entry}
	response, err := req.RequestWith(ctx, producer)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range response.Topics {
		if result.ErrorCode != 0 {
			t.Fatalf("topic error %d", result.ErrorCode)
		}
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		req := kmsg.NewPtrDeleteTopicsRequest()
		req.TopicNames = []string{topic}
		req.RequestWith(cleanup, producer)
	}()
	var mu sync.Mutex
	seen := map[string]int{}
	calls := [2]int{}
	start := func(index int) (*kgo.Client, context.CancelFunc, chan error) {
		client, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
		if err != nil {
			t.Fatal(err)
		}
		workerCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- Consume(workerCtx, client, func(ctx context.Context, record *kgo.Record) error {
				var e AuditCreated
				if err := json.Unmarshal(record.Value, &e); err != nil {
					return err
				}
				if _, err := store.SaveCheck(ctx, e.AuditID, []Finding{}); err != nil {
					return err
				}
				mu.Lock()
				seen[e.AuditID]++
				calls[index]++
				mu.Unlock()
				return nil
			})
		}()
		return client, stop, done
	}
	first, stopFirst, firstDone := start(0)
	second, stopSecond, secondDone := start(1)
	firstStopped := false
	defer func() {
		if !firstStopped {
			stopFirst()
			<-firstDone
			first.Close()
		}
		stopSecond()
		err := <-secondDone
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
		second.Close()
	}()
	publish := func(begin, end int) {
		t.Helper()
		for i := begin; i < end; i++ {
			id := topic + "-" + string(rune('a'+i))
			value, _ := json.Marshal(AuditCreated{EventID: "created-" + id, AuditID: id, URL: "http://localhost:8090/"})
			if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: int32(i % 3), Key: []byte(id), Value: value}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	wait := func(count int) {
		t.Helper()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			mu.Lock()
			n := len(seen)
			mu.Unlock()
			if n == count {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatalf("processed %d, want %d", n, count)
			case <-ticker.C:
			}
		}
	}
	publish(0, 9)
	wait(9)
	mu.Lock()
	both := calls[0] > 0 && calls[1] > 0
	mu.Unlock()
	if !both {
		t.Fatal("records did not reach both members")
	}
	stopFirst()
	err = <-firstDone
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	first.Close()
	firstStopped = true
	publish(9, 12)
	wait(12)
	var count int
	if err = store.DB.QueryRowContext(ctx, `SELECT count(*) FROM results`).Scan(&count); err != nil || count != 12 {
		t.Fatalf("results=%d err=%v", count, err)
	}
}
