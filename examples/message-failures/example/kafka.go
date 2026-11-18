package failures

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

func KafkaPublish(client *kgo.Client) Publish {
	return func(ctx context.Context, topic, key string, value []byte) error {
		return client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: value}).FirstErr()
	}
}

func KafkaCommit(client *kgo.Client) Commit {
	return func(ctx context.Context, record *kgo.Record) error { return client.CommitRecords(ctx, record) }
}

// The client must use DisableAutoCommit and BlockRebalanceOnPoll.
// One record is processed at a time, so later offsets cannot overtake a failure.
func Consume(ctx context.Context, client *kgo.Client, processor Processor) error {
	for {
		fetches := client.PollRecords(ctx, 1)
		if err := ctx.Err(); err != nil {
			return err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("fetch: %w", errs[0].Err)
		}
		for _, record := range fetches.Records() {
			if err := processor.Process(ctx, record); err != nil {
				return err
			}
		}
		client.AllowRebalance()
	}
}
