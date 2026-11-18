package failures

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var ErrTemporary = errors.New("temporary dependency failure")

type Finding struct {
	PageURL string `json:"page_url"`
	Code    string `json:"code"`
}
type AuditChecked struct {
	EventID  string    `json:"event_id"`
	AuditID  string    `json:"audit_id"`
	ResultID string    `json:"result_id"`
	Findings []Finding `json:"findings"`
}

type DeadLetter struct {
	EventID         string `json:"event_id"`
	SourceEventID   string `json:"source_event_id,omitempty"`
	SourceTopic     string `json:"source_topic"`
	SourcePartition int32  `json:"source_partition"`
	SourceOffset    int64  `json:"source_offset"`
	Reason          string `json:"reason"`
	Value           []byte `json:"value_base64"`
}

type Apply func(context.Context, AuditChecked) error
type Publish func(context.Context, string, string, []byte) error
type Commit func(context.Context, *kgo.Record) error

type Processor struct {
	Apply       Apply
	Publish     Publish
	Commit      Commit
	MaxAttempts int
	Wait        func(context.Context, int) error
}

func Pause(ctx context.Context, attempt int) error {
	delay := time.Duration(50*(1<<(attempt-1))+rand.IntN(25)) * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func Decode(value []byte) (AuditChecked, error) {
	var event AuditChecked
	if err := json.Unmarshal(value, &event); err != nil {
		return event, errors.New("invalid JSON or field type")
	}
	if event.EventID == "" || event.AuditID == "" || event.ResultID == "" {
		return event, errors.New("missing event_id, audit_id or result_id")
	}
	return event, nil
}

func (p Processor) Process(ctx context.Context, record *kgo.Record) error {
	if p.MaxAttempts < 1 || p.MaxAttempts > 10 {
		return errors.New("MaxAttempts must be between 1 and 10")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	event, decodeErr := Decode(record.Value)
	if decodeErr != nil {
		// Coordinates remain stable even when the source JSON has no usable event_id.
		letter := DeadLetter{
			EventID:       fmt.Sprintf("dlt:%s:%d:%d", record.Topic, record.Partition, record.Offset),
			SourceEventID: event.EventID, SourceTopic: record.Topic, SourcePartition: record.Partition,
			SourceOffset: record.Offset, Reason: decodeErr.Error(), Value: record.Value,
		}
		bytes, err := json.Marshal(letter)
		if err != nil {
			return err
		}
		if err := p.Publish(ctx, record.Topic+".dlt", letter.EventID, bytes); err != nil {
			return fmt.Errorf("publish DLT: %w", err)
		}
		// Source progress is saved only after the publisher reports a broker ack.
		return p.Commit(ctx, record)
	}
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		err := p.Apply(ctx, event)
		if err == nil {
			return p.Commit(ctx, record)
		}
		if !errors.Is(err, ErrTemporary) {
			return fmt.Errorf("processing failed: %w", err)
		}
		if attempt == p.MaxAttempts {
			return fmt.Errorf("dependency retry exhausted after %d attempts: %w", attempt, err)
		}
		wait := p.Wait
		if wait == nil {
			wait = Pause
		}
		if err := wait(ctx, attempt); err != nil {
			return err
		}
	}
	return errors.New("unreachable processing state")
}
