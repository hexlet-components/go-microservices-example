package telemetry

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

//go:embed data/scenarios.json
var scenariosJSON []byte

type Call struct {
	Service       string `json:"service"`
	Operation     string `json:"operation"`
	Outcome       string `json:"outcome"`
	AuditID       string `json:"audit_id"`
	CorrelationID string `json:"correlation_id"`
	EventID       string `json:"event_id,omitempty"`
	DurationMS    int    `json:"duration_ms"`
	ErrorKind     string `json:"error_kind,omitempty"`
}

type Activity struct {
	Service       string `json:"service"`
	Operation     string `json:"operation"`
	AuditID       string `json:"audit_id"`
	CorrelationID string `json:"correlation_id"`
	ElapsedMS     int    `json:"elapsed_ms"`
}

type Group struct {
	Name      string `json:"name"`
	End       int    `json:"end"`
	Committed int    `json:"committed"`
}

type Outbox struct {
	Service        string `json:"service"`
	CreatedSeconds []int  `json:"created_seconds"`
}

type Finding struct {
	PageURL string `json:"page_url"`
	Code    string `json:"code"`
}

type Report struct {
	AuditID  string    `json:"audit_id"`
	Ready    bool      `json:"ready"`
	Findings []Finding `json:"findings"`
}

type Snapshot struct {
	Source     string          `json:"source"`
	NowSeconds int             `json:"now_seconds"`
	Calls      []Call          `json:"calls"`
	Activities []Activity      `json:"activities"`
	Groups     []Group         `json:"groups"`
	Outboxes   []Outbox        `json:"outboxes"`
	Running    map[string]bool `json:"running"`
	Report     Report          `json:"report"`
}

type Server struct {
	mu        sync.RWMutex
	snapshots []Snapshot
	step      int
}

func NewServer(scenario string) (*Server, error) {
	var scenarios map[string][]Snapshot
	if err := json.Unmarshal(scenariosJSON, &scenarios); err != nil {
		return nil, err
	}
	snapshots, exists := scenarios[scenario]
	if !exists {
		return nil, errors.New("scenario must be slow-checks, broker-down or reports-stopped")
	}
	return &Server{snapshots: snapshots}, nil
}

func LogCall(ctx context.Context, logger *slog.Logger, call Call) {
	attrs := []slog.Attr{
		slog.String("service", call.Service), slog.String("operation", call.Operation),
		slog.String("outcome", call.Outcome), slog.Int("duration_ms", call.DurationMS),
		slog.String("audit_id", call.AuditID), slog.String("correlation_id", call.CorrelationID),
	}
	if call.EventID != "" {
		attrs = append(attrs, slog.String("event_id", call.EventID))
	}
	if call.ErrorKind != "" {
		attrs = append(attrs, slog.String("error_kind", call.ErrorKind))
	}
	level := slog.LevelInfo
	if call.Outcome != "ok" {
		level = slog.LevelError
	}
	logger.LogAttrs(ctx, level, "dependency_call", attrs...)
}

func Measure(ctx context.Context, logger *slog.Logger, call Call, run func(context.Context) error) error {
	start := time.Now()
	err := run(ctx)
	call.DurationMS = int(time.Since(start).Milliseconds())
	call.Outcome = "ok"
	if err != nil {
		call.Outcome = "error"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		call.Outcome = "timeout"
	}
	LogCall(ctx, logger, call)
	return err
}

func lag(group Group) int {
	return max(0, group.End-group.Committed)
}

func oldest(snapshot Snapshot, outbox Outbox) int {
	age := 0
	for _, created := range outbox.CreatedSeconds {
		age = max(age, snapshot.NowSeconds-created)
	}
	return age
}

func metrics(w io.Writer, snapshot Snapshot) {
	type aggregate struct{ count, milliseconds int }
	calls := map[string]aggregate{}
	for _, call := range snapshot.Calls {
		labels := fmt.Sprintf(`service=%q,operation=%q,outcome=%q`, call.Service, call.Operation, call.Outcome)
		current := calls[labels]
		current.count++
		current.milliseconds += call.DurationMS
		calls[labels] = current
	}
	keys := make([]string, 0, len(calls))
	for key := range calls {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, labels := range keys {
		value := calls[labels]
		fmt.Fprintf(w, "audit_dependency_calls_total{%s} %d\n", labels, value.count)
		fmt.Fprintf(w, "audit_dependency_duration_seconds_sum{%s} %.3f\n", labels, float64(value.milliseconds)/1000)
		fmt.Fprintf(w, "audit_dependency_duration_seconds_count{%s} %d\n", labels, value.count)
	}
	for _, group := range snapshot.Groups {
		fmt.Fprintf(w, "audit_consumer_lag_records{consumer_group=%q} %d\n", group.Name, lag(group))
	}
	for _, outbox := range snapshot.Outboxes {
		fmt.Fprintf(w, "audit_outbox_oldest_seconds{service=%q} %d\n", outbox.Service, oldest(snapshot, outbox))
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method == http.MethodPost && r.URL.Path == "/advance" {
		s.step = min(s.step+1, len(s.snapshots)-1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.snapshots[s.step].Report)
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	snapshot := s.snapshots[s.step]
	switch r.URL.Path {
	case "/snapshot":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(snapshot)
	case "/metrics":
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		metrics(w, snapshot)
	case "/diagnostics":
		w.Header().Set("Content-Type", "application/x-ndjson")
		logger := slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.String("time", "2026-11-17T10:00:00Z")
			}
			return attr
		}}))
		logger.Info("fixture_snapshot", "source", snapshot.Source)
		for _, call := range snapshot.Calls {
			LogCall(r.Context(), logger, call)
		}
		for _, activity := range snapshot.Activities {
			logger.Info("operation_running", "service", activity.Service, "operation", activity.Operation,
				"audit_id", activity.AuditID, "correlation_id", activity.CorrelationID, "elapsed_ms", activity.ElapsedMS)
		}
		services := make([]string, 0, len(snapshot.Running))
		for name := range snapshot.Running {
			services = append(services, name)
		}
		sort.Strings(services)
		for _, service := range services {
			logger.Info("service_state", "service", service, "running", snapshot.Running[service])
		}
	case "/reports/a-42":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(snapshot.Report)
	default:
		http.NotFound(w, r)
	}
}

func ScenarioNames() string {
	return strings.Join([]string{"slow-checks", "broker-down", "reports-stopped"}, ", ")
}
