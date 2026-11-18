package lifecycle

import (
	"context"

	"google.golang.org/grpc"
)

func StopGRPC(ctx context.Context, server *grpc.Server) bool {
	done := make(chan struct{})
	go func() { server.GracefulStop(); close(done) }()
	select {
	case <-done:
		return false
	case <-ctx.Done():
		server.Stop()
		return true
	}
}
