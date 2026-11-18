package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("expected observable state was not reached")
		var zero T
		return zero
	}
}

func TestCancelAfterSave(t *testing.T) {
	store := &Store{}
	handler := NewHandler(store)
	handler.AfterSave = make(chan struct{})
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := startRequest(ctx, server.URL)
	await(t, handler.Saved)
	cancel()
	if err := await(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected client cancellation, got %v", err)
	}
	await(t, handler.Finished)
	if store.Count() != 1 {
		t.Fatalf("saved audits: got %d, want 1", store.Count())
	}
}

func TestSuccessfulResponse(t *testing.T) {
	store := &Store{}
	handler := NewHandler(store)
	server := httptest.NewServer(handler)
	defer server.Close()
	response, err := http.Post(server.URL+"/audits", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var payload struct {
		AuditID string `json:"audit_id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusCreated || payload.AuditID != "audit-1" || store.Count() != 1 {
		t.Fatalf("unexpected response: status=%d id=%s count=%d", response.StatusCode, payload.AuditID, store.Count())
	}
}
