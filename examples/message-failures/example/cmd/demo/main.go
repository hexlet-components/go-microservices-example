package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
	failures "hexlet.local/messagefailures"
)

func main() {
	ctx := context.Background()
	valid := &kgo.Record{Topic: "audit.checked", Partition: 0, Offset: 10, Value: []byte(`{"event_id":"e-10","audit_id":"a-10","result_id":"r-10","findings":[]}`)}
	count := 0
	processor := failures.Processor{
		MaxAttempts: 3,
		Wait:        func(context.Context, int) error { return nil },
		Apply: func(context.Context, failures.AuditChecked) error {
			count++
			fmt.Printf("apply attempt %d\n", count)
			if count < 3 {
				return failures.ErrTemporary
			}
			return nil
		},
		Publish: func(ctx context.Context, topic, key string, value []byte) error {
			var letter failures.DeadLetter
			if err := json.Unmarshal(value, &letter); err != nil {
				return err
			}
			fmt.Printf("DLT ack %s %s\n", topic, letter.EventID)
			return nil
		},
		Commit: func(ctx context.Context, record *kgo.Record) error {
			fmt.Printf("commit next offset %d\n", record.Offset+1)
			return nil
		},
	}
	if err := processor.Process(ctx, valid); err != nil {
		log.Fatal(err)
	}
	poison := &kgo.Record{Topic: "audit.checked", Partition: 0, Offset: 11, Value: []byte(`{"event_id":"e-bad"}`)}
	if err := processor.Process(ctx, poison); err != nil {
		log.Fatal(err)
	}
	count = 0
	processor.Apply = func(context.Context, failures.AuditChecked) error { count++; return failures.ErrTemporary }
	if err := processor.Process(ctx, valid); err != nil {
		fmt.Printf("stopped after %d attempts without commit\n", count)
	}
}
