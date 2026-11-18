package isolation

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sony/gobreaker/v2"
)

func TestLimitRejectsWithoutWaiting(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	guard := New(func(ctx context.Context, id string) (Result, error) {
		close(entered)
		select {
		case <-release:
			return Result{AuditID: id}, nil
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	}, 1, time.Second)
	go func() { _, err := guard.Read(context.Background(), "a-1"); finished <- err }()
	<-entered
	_, err := guard.Read(context.Background(), "a-2")
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("error=%v", err)
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func TestCircuitOpensAndRecovers(t *testing.T) {
	var calls atomic.Int32
	guard := New(func(ctx context.Context, id string) (Result, error) {
		if calls.Add(1) <= 2 {
			return Result{}, errors.New("dependency unavailable")
		}
		return Result{AuditID: id}, nil
	}, 1, 10*time.Millisecond)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := guard.Read(ctx, "a-1"); err == nil {
			t.Fatal("expected dependency failure")
		}
	}
	if _, err := guard.Read(ctx, "a-1"); !errors.Is(err, gobreaker.ErrOpenState) {
		t.Fatalf("error=%v", err)
	}
	if calls.Load() != 2 {
		t.Fatal("open circuit called dependency")
	}
	deadline := time.Now().Add(time.Second)
	for {
		result, err := guard.Read(ctx, "a-1")
		if err == nil {
			if result.AuditID != "a-1" {
				t.Fatal(result)
			}
			break
		}
		if !errors.Is(err, gobreaker.ErrOpenState) || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := guard.Read(ctx, "a-2"); err != nil {
		t.Fatal(err)
	}
}

func TestPendingDoesNotOpenCircuit(t *testing.T) {
	var calls int
	guard := New(func(ctx context.Context, id string) (Result, error) { calls++; return Result{}, ErrNotReady }, 1, time.Second)
	for i := 0; i < 4; i++ {
		if _, err := guard.Read(context.Background(), "a-1"); !errors.Is(err, ErrNotReady) {
			t.Fatal(err)
		}
	}
	if calls != 4 {
		t.Fatalf("calls=%d", calls)
	}
}
