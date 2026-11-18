package audit

import (
	"context"
	"errors"
	"testing"
)

func TestLocalContract(t *testing.T) {
	ctx := context.Background()
	client := NewMemoryChecks()
	if _, err := client.Result(ctx, "unknown"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown error=%v", err)
	}
	if err := client.Start(ctx, Audit{ID: "a-42", URL: "http://localhost:8090/"}); err != nil {
		t.Fatal(err)
	}
	state, err := client.State(ctx, "a-42")
	if err != nil || state != CheckRunning {
		t.Fatalf("state=%s error=%v", state, err)
	}
	if _, err = client.Result(ctx, "a-42"); !errors.Is(err, ErrNotReady) {
		t.Fatalf("pending error=%v", err)
	}
	client.Complete(Result{ID: "r-42", AuditID: "a-42", Findings: []Finding{{PageURL: "http://localhost:8090/about", Code: "missing-title"}}})
	result, err := client.Result(ctx, "a-42")
	if err != nil || result.ID != "r-42" || len(result.Findings) != 1 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	state, err = client.State(ctx, "a-42")
	if err != nil || state != CheckDone {
		t.Fatalf("state=%s error=%v", state, err)
	}
}
