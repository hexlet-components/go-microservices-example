package release

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "hexlet.local/independentrelease/checks/v1"
)

type ChecksServer struct {
	pb.UnimplementedChecksServiceServer
	Store Store
}

func (s *ChecksServer) GetResult(ctx context.Context, request *pb.GetResultRequest) (*pb.GetResultResponse, error) {
	findings, err := s.Store.Read(ctx, request.GetAuditId())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "result missing")
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, "checks storage unavailable")
	}
	response := &pb.GetResultResponse{ResultId: "r-" + request.GetAuditId(), AuditId: request.GetAuditId()}
	for _, f := range findings {
		response.Findings = append(response.Findings, &pb.Finding{PageUrl: f.PageURL, Code: f.Code})
	}
	if s.Store.Version == "v2" {
		version, err := s.Store.StoredRuleVersion(ctx, request.GetAuditId())
		if err != nil {
			return nil, status.Error(codes.Unavailable, "result metadata unavailable")
		}
		response.RuleVersion = version
	}
	return response, nil
}

func RunChecks(ctx context.Context, store Store, client *kgo.Client, topic, rpcAddress, httpAddress string) error {
	if store.Version != "v1" && store.Version != "v2" {
		return fmt.Errorf("unsupported version")
	}
	startup, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := store.Prepare(startup)
	if err == nil {
		err = store.Ready(startup)
	}
	cancel()
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", rpcAddress)
	if err != nil {
		return err
	}
	rpcServer := grpc.NewServer()
	pb.RegisterChecksServiceServer(rpcServer, &ChecksServer{Store: store})
	errorsCh := make(chan error, 2)
	go func() { errorsCh <- rpcServer.Serve(listener) }()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if err := store.Ready(r.Context()); err != nil {
			http.Error(w, "schema unavailable", 503)
			return
		}
		fmt.Fprint(w, store.Version)
	})
	mux.HandleFunc("POST /fixture/{id}", func(w http.ResponseWriter, r *http.Request) {
		work, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := store.Fixture(work, r.PathValue("id")); err != nil {
			http.Error(w, "fixture failed", 503)
			return
		}
		w.WriteHeader(204)
	})
	httpServer := &http.Server{Addr: httpAddress, Handler: mux, ReadHeaderTimeout: time.Second}
	go func() { errorsCh <- httpServer.ListenAndServe() }()
	relayCtx, stopRelay := context.WithCancel(ctx)
	defer stopRelay()
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-relayCtx.Done():
				return
			case <-ticker.C:
				work, cancel := context.WithTimeout(relayCtx, time.Second)
				store.Relay(work, client, topic)
				cancel()
			}
		}
	}()
	select {
	case <-ctx.Done():
		err = nil
	case err = <-errorsCh:
	}
	drain, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	httpServer.Shutdown(drain)
	stopped := make(chan struct{})
	go func() { rpcServer.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-drain.Done():
		rpcServer.Stop()
	}
	stopRelay()
	<-relayDone
	return err
}
