package idempotent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

func uniqueName(prefix string) string {
	var bytes [10]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(bytes[:])
}

func isolatedStore(t *testing.T) (Store, string) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Fatal("DATABASE_URL is required for PostgreSQL integration tests")
	}
	schema := uniqueName("dedup_")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := Open(ctx, dsn, schema)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() {
		db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := DropSchema(ctx, dsn, schema); err != nil {
			t.Error(err)
		}
	})
	return Store{DB: db}, schema
}

func checkedEvent() AuditChecked {
	return AuditChecked{
		EventID: "e-42", AuditID: "a-42", ResultID: "r-42",
		Findings: []Finding{{PageURL: "http://localhost:8090/about", Code: "missing-title"}},
	}
}

func assertStatistics(t *testing.T, store Store, applications, markers int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gotApplications, gotMarkers, err := store.Statistics(ctx, "a-42")
	if err != nil {
		t.Fatal(err)
	}
	if gotApplications != applications || gotMarkers != markers {
		t.Fatalf("applications=%d markers=%d, want %d/%d", gotApplications, gotMarkers, applications, markers)
	}
}

func TestDuplicateDelivery(t *testing.T) {
	store, _ := isolatedStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for index := 0; index < 2; index++ {
		applied, err := store.Apply(ctx, "reports-v1", checkedEvent())
		if err != nil {
			t.Fatal(err)
		}
		if applied != (index == 0) {
			t.Fatalf("delivery %d: applied=%t", index, applied)
		}
	}
	assertStatistics(t, store, 1, 1)
}

func TestRollbackAfterMarker(t *testing.T) {
	store, _ := isolatedStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	injected := errors.New("database operation failed before report")
	store.BeforeReport = func() error { return injected }
	if _, err := store.Apply(ctx, "reports-v1", checkedEvent()); !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got %v", err)
	}
	assertStatistics(t, store, 0, 0)
	store.BeforeReport = nil
	applied, err := store.Apply(ctx, "reports-v1", checkedEvent())
	if err != nil || !applied {
		t.Fatalf("replay: applied=%t err=%v", applied, err)
	}
	assertStatistics(t, store, 1, 1)
}

func TestConcurrentDuplicate(t *testing.T) {
	store, _ := isolatedStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const workersCount = 8
	var workers sync.WaitGroup
	errors := make(chan error, workersCount)
	for index := 0; index < workersCount; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := store.Apply(ctx, "reports-v1", checkedEvent())
			errors <- err
		}()
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	assertStatistics(t, store, 1, 1)
}
