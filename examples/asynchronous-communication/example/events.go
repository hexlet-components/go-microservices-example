package auditdemo

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
)

type AuditCreated struct {
	EventID string `json:"event_id"`
	AuditID string `json:"audit_id"`
	URL     string `json:"url"`
}

type AuditChecked struct {
	EventID  string    `json:"event_id"`
	AuditID  string    `json:"audit_id"`
	ResultID string    `json:"result_id"`
	Findings []Finding `json:"findings"`
}

func Publish(ctx context.Context, client *kgo.Client, topic, auditID string, event any) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	record := &kgo.Record{Topic: topic, Key: []byte(auditID), Value: data}
	return client.ProduceSync(ctx, record).FirstErr()
}

func Consume(ctx context.Context, client *kgo.Client, handle func(context.Context, *kgo.Record) error) error {
	for {
		fetches := client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			client.AllowRebalance()
			return ctx.Err()
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			client.AllowRebalance()
			return fmt.Errorf("poll: %w", errs[0].Err)
		}
		for _, record := range fetches.Records() {
			if err := handle(ctx, record); err != nil {
				client.AllowRebalance()
				return err
			}
			if err := client.CommitRecords(ctx, record); err != nil {
				client.AllowRebalance()
				return err
			}
		}
		client.AllowRebalance()
	}
}
