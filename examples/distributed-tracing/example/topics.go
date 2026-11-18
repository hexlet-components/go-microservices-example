package tracedemo

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func CreateTopic(ctx context.Context, brokers []string, topic string) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer client.Close()
	request := kmsg.NewPtrCreateTopicsRequest()
	request.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
	response, err := client.Request(ctx, request)
	if err != nil {
		return err
	}
	for _, t := range response.(*kmsg.CreateTopicsResponse).Topics {
		if t.ErrorCode != 0 {
			return fmt.Errorf("topic error %d", t.ErrorCode)
		}
	}
	return nil
}
func DeleteTopic(ctx context.Context, brokers []string, topic string) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer client.Close()
	request := kmsg.NewPtrDeleteTopicsRequest()
	request.TopicNames = []string{topic}
	_, err = client.Request(ctx, request)
	return err
}
