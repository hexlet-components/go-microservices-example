package idempotent

import (
	"context"
	"encoding/json"

	"github.com/twmb/franz-go/pkg/kgo"
)

func (s Store) HandleRecord(ctx context.Context, client *kgo.Client, record *kgo.Record) error {
	var event AuditChecked
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return err
	}
	if _, err := s.Apply(ctx, "reports-v1", event); err != nil {
		return err
	}
	return client.CommitRecords(ctx, record)
}
