package lifecycle

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	health "google.golang.org/grpc/health/grpc_health_v1"
)

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

type heldHealth struct {
	health.UnimplementedHealthServer
	started chan struct{}
}

func (h heldHealth) Check(ctx context.Context, _ *health.HealthCheckRequest) (*health.HealthCheckResponse, error) {
	close(h.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestGRPCGracefulStopFallsBackToStop(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	started := make(chan struct{})
	health.RegisterHealthServer(server, heldHealth{started: started})
	go server.Serve(listener)
	defer server.Stop()
	client, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := health.NewHealthClient(client).Check(ctx, &health.HealthCheckRequest{})
		result <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	drain, end := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer end()
	if forced := StopGRPC(drain, server); !forced {
		t.Fatal("active RPC was not forced at the deadline")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("forced RPC unexpectedly succeeded")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestHTTPShutdownKeepsAcceptedRequest(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-release:
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
			t.Error("accepted HTTP request canceled during drain")
		}
	}))
	defer server.Close()
	response := make(chan int, 1)
	go func() {
		r, err := http.Get(server.URL)
		if err != nil {
			response <- 0
			return
		}
		r.Body.Close()
		response <- r.StatusCode
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP request did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- server.Config.Shutdown(ctx) }()
	close(release)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if code := <-response; code != http.StatusOK {
		t.Fatalf("accepted status=%d", code)
	}
}
