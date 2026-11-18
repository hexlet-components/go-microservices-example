package retrydemo

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func noWait(context.Context, time.Duration) error { return nil }
func noJitter(time.Duration) time.Duration        { return 0 }

func TestTransientReadRecovers(t *testing.T) {
	attempts := 0
	err := retryRead(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("read result: %w", ErrUnavailable)
		}
		return nil
	}, noWait, noJitter)
	if err != nil || attempts != 3 {
		t.Fatalf("attempts=%d error=%v", attempts, err)
	}
}

func TestAttemptsAreBounded(t *testing.T) {
	attempts, pauses := 0, 0
	err := retryRead(context.Background(), func(context.Context) error { attempts++; return ErrUnavailable }, func(context.Context, time.Duration) error { pauses++; return nil }, noJitter)
	if !errors.Is(err, ErrUnavailable) || attempts != 3 || pauses != 2 {
		t.Fatalf("attempts=%d pauses=%d error=%v", attempts, pauses, err)
	}
}

func TestPermanentErrorIsNotRepeated(t *testing.T) {
	attempts := 0
	missing := errors.New("audit not found")
	err := retryRead(context.Background(), func(context.Context) error { attempts++; return missing }, func(context.Context, time.Duration) error { t.Fatal("unexpected pause"); return nil }, noJitter)
	if !errors.Is(err, missing) || attempts != 1 {
		t.Fatalf("attempts=%d error=%v", attempts, err)
	}
}

func TestCancellationDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiting := make(chan struct{})
	done := make(chan error, 1)
	attempts := 0
	go func() {
		done <- retryRead(ctx, func(context.Context) error { attempts++; return ErrUnavailable }, func(ctx context.Context, delay time.Duration) error {
			close(waiting)
			return wait(ctx, delay)
		}, func(time.Duration) time.Duration { return time.Hour })
	}()
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("did not enter backoff")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || attempts != 1 {
			t.Fatalf("attempts=%d error=%v", attempts, err)
		}
	case <-time.After(time.Second):
		t.Fatal("backoff ignored cancellation")
	}
}

func TestOneDeadlineCoversAllAttempts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	attempts := 0
	started := time.Now()
	err := retryRead(ctx, func(callCtx context.Context) error {
		attempts++
		if callCtx != ctx {
			t.Fatal("attempt received another context")
		}
		return ErrUnavailable
	}, wait, func(limit time.Duration) time.Duration { return limit })
	if !errors.Is(err, context.DeadlineExceeded) || attempts != 1 {
		t.Fatalf("attempts=%d error=%v", attempts, err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("caller deadline was renewed")
	}
}
