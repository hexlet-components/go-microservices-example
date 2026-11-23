package auditdemo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func testStore(t *testing.T, role string) Store {
	t.Helper()
	dsn := os.Getenv("OUTBOX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("OUTBOX_TEST_DATABASE_URL is required")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	id, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	schema := "outbox_test_" + id
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	db, err := sql.Open("pgx", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(); admin.Exec("DROP SCHEMA " + schema + " CASCADE"); admin.Close() })
	store := Store{DB: db}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.Migrate(ctx, role); err != nil {
		t.Fatal(err)
	}
	return store
}

func countRows(t *testing.T, s Store, table string) int {
	t.Helper()
	var count int
	if err := s.DB.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestCreateIsAtomicAndRetainsIdentity(t *testing.T) {
	store := testStore(t, "api")
	ctx := context.Background()
	first, err := store.Create(ctx, 1, "key", "http://127.0.0.1:8090/", "created-test")
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.Create(ctx, 1, "key", "http://127.0.0.1:8090/", "created-test")
	if err != nil {
		t.Fatal(err)
	}
	if first != again || countRows(t, store, "audits") != 1 || countRows(t, store, "outbox") != 1 {
		t.Fatal("repeated creation changed identity or count")
	}
	if _, err := store.Create(ctx, 1, "key", "http://127.0.0.1:8090/about", "created-test"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
}

func TestOutboxFailureRollsBackDomainWrite(t *testing.T) {
	for _, role := range []string{"api", "checks"} {
		t.Run(role, func(t *testing.T) {
			store := testStore(t, role)
			_, err := store.DB.Exec(`CREATE FUNCTION reject_outbox() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'outbox failure';END$$;CREATE TRIGGER reject_outbox BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION reject_outbox()`)
			if err != nil {
				t.Fatal(err)
			}
			table := "audits"
			if role == "api" {
				_, err = store.Create(context.Background(), 1, "key", "http://127.0.0.1:8090/", "created-test")
			} else {
				table = "results"
				_, err = store.SaveCheck(context.Background(), "audit-1", []Finding{}, "checked-test")
			}
			if err == nil {
				t.Fatal("expected failing outbox insert")
			}
			if countRows(t, store, table) != 0 || countRows(t, store, "outbox") != 0 {
				t.Fatal("partial transaction committed")
			}
		})
	}
}

func TestConcurrentCheckUsesWinnerIdentifiers(t *testing.T) {
	store := testStore(t, "checks")
	const attempts = 8
	events := make(chan AuditChecked, attempts)
	failures := make(chan error, attempts)
	var group sync.WaitGroup
	for i := 0; i < attempts; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			event, err := store.SaveCheck(context.Background(), "audit-shared", []Finding{{PageURL: "/", Code: fmt.Sprintf("rule-%d", i)}}, "checked-test")
			if err != nil {
				failures <- err
			} else {
				events <- event
			}
		}(i)
	}
	group.Wait()
	close(events)
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	var winner AuditChecked
	for event := range events {
		if winner.EventID != "" && !reflect.DeepEqual(winner, event) {
			t.Fatalf("different terminal events: %#v %#v", winner, event)
		}
		winner = event
	}
	if countRows(t, store, "results") != 1 || countRows(t, store, "outbox") != 1 {
		t.Fatal("more than one terminal result or outbox event")
	}
	envelope, found, err := store.Next(context.Background())
	if err != nil || !found {
		t.Fatal(err)
	}
	var durable AuditChecked
	if err := json.Unmarshal(envelope.Payload, &durable); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(winner, durable) {
		t.Fatal("outbox differs from canonical result")
	}
}

func kafkaTopic(t *testing.T) ([]string, string) {
	t.Helper()
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	if brokers[0] == "" {
		t.Skip("KAFKA_BROKERS is required")
	}
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	id, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	topic := "outbox-test-" + id
	request := kmsg.NewPtrCreateTopicsRequest()
	request.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	response, err := admin.Request(ctx, request)
	cancel()
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	for _, topicResponse := range response.(*kmsg.CreateTopicsResponse).Topics {
		if topicResponse.ErrorCode != 0 {
			admin.Close()
			t.Fatalf("create topic error %d", topicResponse.ErrorCode)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		deletion := kmsg.NewPtrDeleteTopicsRequest()
		deletion.TopicNames = []string{topic}
		admin.Request(ctx, deletion)
		admin.Close()
	})
	return brokers, topic
}

func publisher(t *testing.T, brokers []string) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func readRecords(t *testing.T, brokers []string, topic string, count int) []*kgo.Record {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var records []*kgo.Record
	for len(records) < count {
		fetches := client.PollRecords(ctx, count-len(records))
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		records = append(records, fetches.Records()...)
	}
	return records
}

func TestUnavailableBrokerLeavesOutbox(t *testing.T) {
	store := testStore(t, "api")
	brokers, topic := kafkaTopic(t)
	event, err := store.Create(context.Background(), 1, "key", "http://127.0.0.1:8090/", topic)
	if err != nil {
		t.Fatal(err)
	}
	broken := publisher(t, []string{"127.0.0.1:1"})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	sent, err := (Sender{Store: store, Publisher: KafkaPublisher{Client: broken}}).Step(ctx)
	cancel()
	broken.Close()
	if err == nil || sent {
		t.Fatal("unavailable broker cannot mark an event sent")
	}
	if count, err := store.Pending(context.Background()); err != nil || count != 1 {
		t.Fatalf("pending=%d error=%v", count, err)
	}
	good := publisher(t, brokers)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sent, err = (Sender{Store: store, Publisher: KafkaPublisher{Client: good}}).Step(ctx)
	if err != nil || !sent {
		t.Fatal(err)
	}
	if count, err := store.Pending(ctx); err != nil || count != 0 {
		t.Fatalf("pending=%d error=%v", count, err)
	}
	record := readRecords(t, brokers, topic, 1)[0]
	var actual AuditCreated
	if err := json.Unmarshal(record.Value, &actual); err != nil || actual != event {
		t.Fatalf("event=%+v error=%v", actual, err)
	}
}

func TestRestartAfterAckRepeatsStableEvent(t *testing.T) {
	checks := testStore(t, "checks")
	reports := testStore(t, "reports")
	brokers, topic := kafkaTopic(t)
	event, err := checks.SaveCheck(context.Background(), "audit-1", []Finding{{PageURL: "/", Code: "missing-content-security-policy"}}, topic)
	if err != nil {
		t.Fatal(err)
	}
	first := publisher(t, brokers)
	sender := Sender{Store: checks, Publisher: KafkaPublisher{Client: first}, AfterAck: func(Envelope) error { return ErrStopAfterAck }}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := sender.Step(ctx); !errors.Is(err, ErrStopAfterAck) {
		t.Fatal(err)
	}
	first.Close()
	if count, err := checks.Pending(ctx); err != nil || count != 1 {
		t.Fatalf("pending=%d error=%v", count, err)
	}
	second := publisher(t, brokers)
	sent, err := (Sender{Store: checks, Publisher: KafkaPublisher{Client: second}}).Step(ctx)
	if err != nil || !sent {
		t.Fatal(err)
	}
	for _, record := range readRecords(t, brokers, topic, 2) {
		var duplicate AuditChecked
		if err := json.Unmarshal(record.Value, &duplicate); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(duplicate, event) {
			t.Fatal("restart changed event or result identity")
		}
		if err := reports.ApplyReport(ctx, duplicate); err != nil {
			t.Fatal(err)
		}
	}
	if countRows(t, reports, "reports") != 1 || countRows(t, reports, "processed_events") != 1 {
		t.Fatal("duplicate event produced extra state")
	}
	var applications int
	var resultID string
	if err := reports.DB.QueryRow("SELECT applications,result_id FROM reports").Scan(&applications, &resultID); err != nil {
		t.Fatal(err)
	}
	if applications != 1 || resultID != event.ResultID {
		t.Fatalf("applications=%d result=%s", applications, resultID)
	}
}
