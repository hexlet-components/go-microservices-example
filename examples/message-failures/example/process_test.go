package failures

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func validRecord() *kgo.Record {
	return &kgo.Record{Topic: "audit.checked", Partition: 2, Offset: 7, Value: []byte(`{"event_id":"e-7","audit_id":"a-7","result_id":"r-7","findings":[]}`)}
}
func noWait(context.Context, int) error { return nil }

func TestRetrySameRecordBeforeCommit(t *testing.T) {
	var calls []string
	attempt := 0
	processor := Processor{MaxAttempts: 3, Wait: noWait,
		Apply: func(ctx context.Context, event AuditChecked) error {
			calls = append(calls, event.EventID)
			attempt++
			if attempt < 3 {
				return ErrTemporary
			}
			return nil
		},
		Publish: func(context.Context, string, string, []byte) error { t.Fatal("unexpected DLT"); return nil },
		Commit:  func(context.Context, *kgo.Record) error { calls = append(calls, "commit"); return nil },
	}
	if err := processor.Process(context.Background(), validRecord()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"e-7", "e-7", "e-7", "commit"}) {
		t.Fatalf("calls=%v", calls)
	}
}

func TestDependencyExhaustionNeverCommitsOrDeadLetters(t *testing.T) {
	attempts, commits, published := 0, 0, 0
	processor := Processor{MaxAttempts: 3, Wait: noWait,
		Apply:   func(context.Context, AuditChecked) error { attempts++; return ErrTemporary },
		Publish: func(context.Context, string, string, []byte) error { published++; return nil },
		Commit:  func(context.Context, *kgo.Record) error { commits++; return nil },
	}
	if err := processor.Process(context.Background(), validRecord()); !errors.Is(err, ErrTemporary) {
		t.Fatalf("error=%v", err)
	}
	if attempts != 3 || commits != 0 || published != 0 {
		t.Fatalf("attempts=%d commits=%d published=%d", attempts, commits, published)
	}
}

func TestDeadLetterAckPrecedesSourceCommit(t *testing.T) {
	record := validRecord()
	record.Value = []byte(`{"event_id":"e-bad"}`)
	var calls []string
	var letter DeadLetter
	processor := Processor{MaxAttempts: 3, Wait: noWait,
		Apply: func(context.Context, AuditChecked) error { t.Fatal("invalid event was applied"); return nil },
		Publish: func(ctx context.Context, topic, key string, value []byte) error {
			calls = append(calls, "ack")
			if err := json.Unmarshal(value, &letter); err != nil {
				return err
			}
			if topic != "audit.checked.dlt" || key != letter.EventID {
				t.Fatalf("topic=%s key=%s", topic, key)
			}
			return nil
		},
		Commit: func(context.Context, *kgo.Record) error { calls = append(calls, "commit"); return nil },
	}
	if err := processor.Process(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"ack", "commit"}) {
		t.Fatalf("calls=%v", calls)
	}
	if letter.SourceTopic != record.Topic || letter.SourcePartition != 2 || letter.SourceOffset != 7 || letter.SourceEventID != "e-bad" || string(letter.Value) != string(record.Value) {
		t.Fatalf("letter=%+v", letter)
	}
}

func TestFailedDLTPublishDoesNotCommit(t *testing.T) {
	record := validRecord()
	record.Value = []byte("invalid JSON")
	commits := 0
	processor := Processor{MaxAttempts: 3, Publish: func(context.Context, string, string, []byte) error { return errors.New("broker unavailable") }, Commit: func(context.Context, *kgo.Record) error { commits++; return nil }}
	if err := processor.Process(context.Background(), record); err == nil || commits != 0 {
		t.Fatalf("error=%v commits=%d", err, commits)
	}
}

func TestWindowAfterDLTAckProducesSameIdentity(t *testing.T) {
	record := validRecord()
	record.Value = []byte("invalid JSON")
	var identities []string
	processor := Processor{MaxAttempts: 3, Publish: func(ctx context.Context, topic, key string, value []byte) error {
		identities = append(identities, key)
		return nil
	}, Commit: func(context.Context, *kgo.Record) error { return errors.New("interrupted before source commit") }}
	if err := processor.Process(context.Background(), record); err == nil {
		t.Fatal("expected commit failure")
	}
	processor.Commit = func(context.Context, *kgo.Record) error { return nil }
	if err := processor.Process(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if len(identities) != 2 || identities[0] != identities[1] {
		t.Fatalf("identities=%v", identities)
	}
}

func TestCancelDuringRetryDoesNotCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commits := 0
	processor := Processor{MaxAttempts: 3, Apply: func(context.Context, AuditChecked) error { return ErrTemporary }, Wait: func(ctx context.Context, attempt int) error { cancel(); return ctx.Err() }, Commit: func(context.Context, *kgo.Record) error { commits++; return nil }}
	if err := processor.Process(ctx, validRecord()); !errors.Is(err, context.Canceled) || commits != 0 {
		t.Fatalf("error=%v commits=%d", err, commits)
	}
}
