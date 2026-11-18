package release

import (
	"context"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func Env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
func PrepareTopic(ctx context.Context, client *kgo.Client, topic string) error {
	request := kmsg.NewPtrCreateTopicsRequest()
	request.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
	response, err := client.Request(ctx, request)
	if err != nil {
		return err
	}
	for _, row := range response.(*kmsg.CreateTopicsResponse).Topics {
		if row.ErrorCode != 0 && row.ErrorCode != 36 {
			return fmt.Errorf("topic setup error %d", row.ErrorCode)
		}
	}
	return nil
}
