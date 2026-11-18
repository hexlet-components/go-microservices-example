package audit

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	pb "hexlet.local/checksrpc/checks/v1"
)

func testReader(t *testing.T) (*RPCReader, pb.ChecksServiceClient) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterChecksServiceServer(server, &RPCServer{Reader: FixtureReader{}})
	go server.Serve(listener)
	t.Cleanup(server.GracefulStop)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	client := pb.NewChecksServiceClient(connection)
	return &RPCReader{Client: client}, client
}

func TestRPCResultContract(t *testing.T) {
	reader, _ := testReader(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := reader.Result(ctx, "a-41")
	if err != nil {
		t.Fatal(err)
	}
	if result.ID != "r-41" || result.AuditID != "a-41" || len(result.Findings) != 1 || result.Findings[0].Code != "missing-title" {
		t.Fatalf("result=%+v", result)
	}
	_, err = reader.Result(ctx, "a-42")
	if !errors.Is(err, ErrNotReady) {
		t.Fatalf("pending error=%v", err)
	}
	_, err = reader.Result(ctx, "unknown")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing error=%v", err)
	}
}

func TestRPCWireStatuses(t *testing.T) {
	_, client := testReader(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, row := range []struct {
		id   string
		want codes.Code
	}{{"", codes.InvalidArgument}, {"a-42", codes.FailedPrecondition}, {"unknown", codes.NotFound}} {
		_, err := client.GetResult(ctx, &pb.GetResultRequest{AuditId: row.id})
		if status.Code(err) != row.want {
			t.Errorf("id=%q status=%s want=%s", row.id, status.Code(err), row.want)
		}
	}
}

type testHTTP struct{ started bool }

func (h *testHTTP) Start(context.Context, Audit) error { h.started = true; return nil }
func (h *testHTTP) State(ctx context.Context, id string) (CheckState, error) {
	return FixtureReader{}.State(ctx, id)
}

func TestHybridKeepsStartAndState(t *testing.T) {
	reader, _ := testReader(t)
	httpClient := &testHTTP{}
	client := &ChecksClient{HTTP: httpClient, RPC: reader}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Start(ctx, Audit{ID: "a-42"}); err != nil || !httpClient.started {
		t.Fatalf("start=%v error=%v", httpClient.started, err)
	}
	state, err := client.State(ctx, "a-42")
	if err != nil || state != CheckRunning {
		t.Fatalf("state=%s error=%v", state, err)
	}
	if _, err := client.Result(ctx, "a-42"); !errors.Is(err, ErrNotReady) {
		t.Fatalf("pending result error=%v", err)
	}
}
