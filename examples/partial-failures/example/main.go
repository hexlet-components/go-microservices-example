package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"
)

type Store struct {
	mu    sync.Mutex
	count int
}

func (s *Store) Create() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	return fmt.Sprintf("audit-%d", s.count)
}

func (s *Store) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

type Handler struct {
	Store      *Store
	BeforeSave <-chan struct{}
	AfterSave  <-chan struct{}
	Started    chan struct{}
	Saved      chan struct{}
	Finished   chan struct{}
}

func NewHandler(store *Store) *Handler {
	return &Handler{
		Store:    store,
		Started:  make(chan struct{}, 1),
		Saved:    make(chan struct{}, 1),
		Finished: make(chan struct{}, 1),
	}
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// A nil gate continues immediately. A non-nil gate delays the selected stage.
func wait(ctx context.Context, gate <-chan struct{}) error {
	if gate == nil {
		return ctx.Err()
	}
	select {
	case <-gate:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/audits" {
		http.NotFound(w, r)
		return
	}
	signal(h.Started)
	defer signal(h.Finished)
	if err := wait(r.Context(), h.BeforeSave); err != nil {
		return
	}
	id := h.Store.Create()
	signal(h.Saved)
	if err := wait(r.Context(), h.AfterSave); err != nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"audit_id": id})
}

func startRequest(ctx context.Context, address string) <-chan error {
	result := make(chan error, 1)
	go func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, address+"/audits", nil)
		if err != nil {
			result <- err
			return
		}
		response, err := http.DefaultClient.Do(req)
		if response != nil {
			response.Body.Close()
		}
		result <- err
	}()
	return result
}

func run() error {
	store := &Store{}
	handler := NewHandler(store)
	handler.AfterSave = make(chan struct{})
	server := httptest.NewServer(handler)
	defer server.Close()
	parent, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	result := startRequest(ctx, server.URL)
	select {
	case <-handler.Saved:
		cancel()
	case <-parent.Done():
		return fmt.Errorf("request did not reach storage: %w", parent.Err())
	}
	select {
	case err := <-result:
		fmt.Printf("client_canceled=%t saved_audits=%d\n", errors.Is(err, context.Canceled), store.Count())
		return nil
	case <-parent.Done():
		return parent.Err()
	}
}

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}
