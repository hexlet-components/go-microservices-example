package auditdemo

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var ErrStopAfterAck = errors.New("controlled stop after broker ack")

type Publisher interface {
	Publish(context.Context, Envelope) error
}
type KafkaPublisher struct{ Client *kgo.Client }

func (p KafkaPublisher) Publish(ctx context.Context, e Envelope) error {
	return p.Client.ProduceSync(ctx, &kgo.Record{Topic: e.Topic, Key: []byte(e.AuditID), Value: e.Payload}).FirstErr()
}

type Sender struct {
	Store     Store
	Publisher Publisher
	AfterAck  func(Envelope) error
}

// One sender is active for this database. Network publication holds no SQL transaction.
func (s Sender) Step(ctx context.Context) (bool, error) {
	event, found, err := s.Store.Next(ctx)
	if err != nil || !found {
		return false, err
	}
	if err := s.Publisher.Publish(ctx, event); err != nil {
		return false, err
	}
	if s.AfterAck != nil {
		if err := s.AfterAck(event); err != nil {
			return false, err
		}
	}
	if err := s.Store.MarkSent(ctx, event.EventID); err != nil {
		return false, err
	}
	return true, nil
}

func (s Sender) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
		sent, err := s.Step(attempt)
		cancel()
		if errors.Is(err, ErrStopAfterAck) {
			return err
		}
		if err != nil && ctx.Err() == nil {
			slog.Warn("outbox_publication_failed", "error", err)
		}
		if sent {
			continue
		}
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
