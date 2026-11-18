package auditdemo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestLostResponseRetainsAudit(t *testing.T) {
	service, target := testService(t)
	secret := "local-example-secret-32-characters"
	token, err := Token(secret, 1)
	if err != nil {
		t.Fatal(err)
	}
	api := API(service, secret)
	var calls atomic.Int32
	saved := make(chan Audit, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			audit, err := service.Create(r.Context(), 1, r.Header.Get("Idempotency-Key"), target)
			if err != nil {
				t.Errorf("first creation: %v", err)
				http.Error(w, "failed", 500)
				return
			}
			saved <- audit
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			connection.Close()
			return
		}
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	send := func() (string, error) {
		request, err := http.NewRequest(http.MethodPost, server.URL+"/audits", strings.NewReader(fmt.Sprintf(`{"url":%q}`, target)))
		if err != nil {
			return "", err
		}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Idempotency-Key", "lost-response")
		response, err := server.Client().Do(request)
		if err != nil {
			return "", err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusCreated {
			return "", fmt.Errorf("HTTP %d", response.StatusCode)
		}
		var created struct {
			ID string `json:"audit_id"`
		}
		err = json.NewDecoder(response.Body).Decode(&created)
		return created.ID, err
	}
	if _, err := send(); err == nil {
		t.Fatal("first response must be lost")
	}
	first := <-saved
	second, err := send()
	if err != nil {
		t.Fatal(err)
	}
	if second != first.ID {
		t.Fatalf("first=%s second=%s", first.ID, second)
	}
	report, err := service.Report(context.Background(), 1, second)
	if err != nil || !report.Ready {
		t.Fatalf("report=%+v error=%v", report, err)
	}
	var count int
	if err := service.Store.DB.QueryRow("SELECT COUNT(*) FROM audits").Scan(&count); err != nil || count != 1 {
		t.Fatalf("count=%d error=%v", count, err)
	}
}
