package retrydemo

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

var ErrUnavailable = errors.New("dependency unavailable")

func RetryRead(ctx context.Context, read func(context.Context) error) error {
	return retryRead(ctx, read, wait, func(limit time.Duration) time.Duration {
		return time.Duration(rand.Int64N(int64(limit) + 1))
	})
}

func retryRead(ctx context.Context, read func(context.Context) error, pause func(context.Context, time.Duration) error, jitter func(time.Duration) time.Duration) error {
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := read(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !errors.Is(err, ErrUnavailable) || attempt == maxAttempts-1 {
			return err
		}
		limit := (50 * time.Millisecond) << attempt
		if err := pause(ctx, jitter(limit)); err != nil {
			return err
		}
	}
	return nil
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
