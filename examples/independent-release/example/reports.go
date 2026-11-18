package release

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func RunReports(ctx context.Context, client *kgo.Client, address string) error {
	var mutex sync.RWMutex
	reports := map[string]Report{}
	seen := map[string]bool{}
	errorsCh := make(chan error, 2)
	go func() {
		for {
			fetched := client.PollRecords(ctx, 1)
			if ctx.Err() != nil {
				return
			}
			if errs := fetched.Errors(); len(errs) > 0 {
				errorsCh <- errs[0].Err
				return
			}
			for _, record := range fetched.Records() {
				report, err := ConsumeV1(record.Value)
				if err != nil {
					errorsCh <- err
					return
				}
				var event EventV1
				if err := json.Unmarshal(record.Value, &event); err != nil {
					errorsCh <- err
					return
				}
				mutex.Lock()
				if !seen[event.EventID] {
					reports[event.AuditID] = report
					seen[event.EventID] = true
				}
				mutex.Unlock()
				if err := client.CommitRecords(ctx, record); err != nil {
					errorsCh <- err
					return
				}
			}
			client.AllowRebalance()
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "reports-v1") })
	mux.HandleFunc("GET /reports/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		mutex.RLock()
		report, exists := reports[id]
		mutex.RUnlock()
		if !exists {
			report = Report{AuditID: id, Findings: []Finding{}}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(report)
	})
	server := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: time.Second}
	go func() { errorsCh <- server.ListenAndServe() }()
	var err error
	select {
	case <-ctx.Done():
	case err = <-errorsCh:
	}
	drain, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	server.Shutdown(drain)
	return err
}
