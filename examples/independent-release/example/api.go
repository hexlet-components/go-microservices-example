package release

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/sony/gobreaker/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	pb "hexlet.local/independentrelease/checks/v1"
)

func API(checks pb.ChecksServiceClient, reportsURL string) http.Handler {
	breaker := gobreaker.NewCircuitBreaker[*pb.GetResultResponse](gobreaker.Settings{Name: "checks", MaxRequests: 1, Timeout: 150 * time.Millisecond, ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= 2 }, IsSuccessful: func(err error) bool {
		return err == nil || status.Code(err) == codes.NotFound || status.Code(err) == codes.FailedPrecondition
	}})
	httpClient := &http.Client{Timeout: time.Second}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /diagnostics", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"checks_circuit": breaker.State().String()})
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "api-v1") })
	mux.HandleFunc("GET /audits/{id}/result", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 200*time.Millisecond)
		defer cancel()
		response, err := breaker.Execute(func() (*pb.GetResultResponse, error) {
			return checks.GetResult(ctx, &pb.GetResultRequest{AuditId: r.PathValue("id")})
		})
		if err != nil {
			if errors.Is(err, gobreaker.ErrOpenState) {
				w.Header().Set("Retry-After", "1")
			}
			code := 503
			if status.Code(err) == codes.NotFound {
				code = 404
			}
			http.Error(w, http.StatusText(code), code)
			return
		}
		findings := make([]Finding, 0, len(response.Findings))
		for _, f := range response.Findings {
			findings = append(findings, Finding{PageURL: f.PageUrl, Code: f.Code})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"audit_id": response.AuditId, "result_id": response.ResultId, "findings": findings})
	})
	mux.HandleFunc("GET /audits/{id}/report", func(w http.ResponseWriter, r *http.Request) {
		request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, reportsURL+"/reports/"+r.PathValue("id"), nil)
		if err != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		response, err := httpClient.Do(request)
		if err != nil {
			http.Error(w, "reports unavailable", 503)
			return
		}
		defer response.Body.Close()
		var report Report
		if err := json.NewDecoder(response.Body).Decode(&report); err != nil {
			http.Error(w, "invalid report", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(report)
	})
	return mux
}

func RunAPI(ctx context.Context, checksAddress, reportsURL, address string) error {
	connection, err := grpc.NewClient(checksAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer connection.Close()
	server := &http.Server{Addr: address, Handler: API(pb.NewChecksServiceClient(connection), reportsURL), ReadHeaderTimeout: time.Second}
	errorsCh := make(chan error, 1)
	go func() { errorsCh <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
	case err = <-errorsCh:
	}
	drain, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	server.Shutdown(drain)
	return err
}
