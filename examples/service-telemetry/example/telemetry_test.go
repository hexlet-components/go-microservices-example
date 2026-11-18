package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestScenariosReachAnObservableReport(t *testing.T) {
	checks := map[string]string{
		"slow-checks":     `audit_consumer_lag_records{consumer_group="checks-v1"} 1`,
		"broker-down":     `audit_outbox_oldest_seconds{service="api"} 90`,
		"reports-stopped": `audit_consumer_lag_records{consumer_group="reports-v1"} 1`,
	}
	for scenario, expected := range checks {
		t.Run(scenario, func(t *testing.T) {
			handler, err := NewServer(scenario)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			readReport := func() Report {
				response, err := http.Get(server.URL + "/reports/a-42")
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				var report Report
				if err := json.NewDecoder(response.Body).Decode(&report); err != nil {
					t.Fatal(err)
				}
				return report
			}
			if readReport().Ready {
				t.Fatal("report must be pending before recovery")
			}
			response, err := http.Get(server.URL + "/metrics")
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil || !strings.Contains(string(data), expected) {
				t.Fatalf("missing diagnostic metric %q, err=%v: %s", expected, err, data)
			}
			response, err = http.Post(server.URL+"/advance", "application/json", nil)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			report := readReport()
			if !report.Ready || len(report.Findings) != 2 {
				t.Fatalf("expected completed report, got %+v", report)
			}
		})
	}
}

func TestMetricLabelsDoNotGrowWithAuditIDs(t *testing.T) {
	snapshot := Snapshot{}
	for index := 0; index < 1000; index++ {
		snapshot.Calls = append(snapshot.Calls, Call{
			Service: "checks", Operation: "check_site", Outcome: "ok",
			AuditID: fmt.Sprintf("audit-%d", index), CorrelationID: fmt.Sprintf("request-%d", index), DurationMS: 5,
		})
	}
	var output bytes.Buffer
	metrics(&output, snapshot)
	text := output.String()
	if strings.Count(text, "audit_dependency_calls_total{") != 1 {
		t.Fatalf("audit identifiers created separate metric series: %s", text)
	}
	for _, forbidden := range []string{"audit_id=", "correlation_id=", "event_id=", "url=", "user="} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("forbidden metric label: %s", forbidden)
		}
	}
	if !strings.Contains(text, "} 1000") {
		t.Fatalf("missing total: %s", text)
	}
}

func TestStructuredLogUsesSelectedFields(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	err := Measure(context.Background(), logger, Call{
		Service: "api", Operation: "get_report", AuditID: "a-42", CorrelationID: "corr-a-42", ErrorKind: "dependency_unavailable",
	}, func(context.Context) error { return errors.New("password=synthetic-password bearer=synthetic-token") })
	if err == nil {
		t.Fatal("dependency error was lost")
	}
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"service", "operation", "audit_id", "correlation_id", "duration_ms", "outcome"} {
		if _, exists := record[field]; !exists {
			t.Fatalf("missing structured field: %s", field)
		}
	}
	if record["outcome"] != "error" || record["level"] != "ERROR" {
		t.Fatalf("unexpected outcome: %v", record)
	}
	if strings.Contains(output.String(), "synthetic-") {
		t.Fatal("raw dependency error exposed sensitive values")
	}
}

func TestLagAndOldestPendingAge(t *testing.T) {
	if lag(Group{End: 5, Committed: 4}) != 1 {
		t.Fatal("unexpected lag")
	}
	snapshot := Snapshot{NowSeconds: 100}
	if oldest(snapshot, Outbox{}) != 0 || oldest(snapshot, Outbox{CreatedSeconds: []int{10, 20}}) != 90 {
		t.Fatal("unexpected pending outbox age")
	}
}
