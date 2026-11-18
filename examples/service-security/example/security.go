package security

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	pb "hexlet.local/servicesecurity/reports/v1"
)

type ServiceIdentity struct {
	Token          string
	CanReadReports bool
}

type TokenCredentials struct{ Token string }

func (c TokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.Token}, nil
}
func (TokenCredentials) RequireTransportSecurity() bool { return true }

func Authorize(identities []ServiceIdentity) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		values := metadata.ValueFromIncomingContext(ctx, "authorization")
		if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
			return nil, status.Error(codes.Unauthenticated, "service token required")
		}
		token := strings.TrimPrefix(values[0], "Bearer ")
		for _, identity := range identities {
			if len(identity.Token) > 0 && subtle.ConstantTimeCompare([]byte(identity.Token), []byte(token)) == 1 {
				if !identity.CanReadReports || info.FullMethod != pb.ReportsService_GetReport_FullMethodName {
					return nil, status.Error(codes.PermissionDenied, "method not allowed")
				}
				return next(ctx, request)
			}
		}
		return nil, status.Error(codes.Unauthenticated, "unknown service token")
	}
}

func Diagnostics(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		started := time.Now()
		response, err := next(ctx, request)
		logger.InfoContext(ctx, "rpc_completed", "method", info.FullMethod, "code", status.Code(err).String(), "duration_ms", time.Since(started).Milliseconds())
		return response, err
	}
}

func NewServer(certFile, keyFile string, identities []ServiceIdentity, logger *slog.Logger) (*grpc.Server, error) {
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	if len(identities) != 2 || len(identities[0].Token) < 16 || len(identities[1].Token) < 16 || identities[0].Token == identities[1].Token {
		return nil, errors.New("two distinct service tokens of at least 16 bytes are required")
	}
	server := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12})),
		grpc.ChainUnaryInterceptor(Diagnostics(logger), Authorize(identities)),
	)
	pb.RegisterReportsServiceServer(server, &ReportsServer{})
	return server, nil
}

func Dial(address, caFile, serverName, token string) (*grpc.ClientConn, error) {
	certificate, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		return nil, errors.New("invalid server CA certificate")
	}
	return grpc.NewClient(address,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: serverName, MinVersion: tls.VersionTLS12})),
		grpc.WithPerRPCCredentials(TokenCredentials{Token: token}),
	)
}

type ReportsServer struct {
	pb.UnimplementedReportsServiceServer
}

func (*ReportsServer) GetReport(ctx context.Context, request *pb.GetReportRequest) (*pb.GetReportResponse, error) {
	switch request.GetAuditId() {
	case "a-1", "a-2":
		return &pb.GetReportResponse{AuditId: request.GetAuditId(), Ready: true, Findings: []*pb.Finding{{PageUrl: "http://127.0.0.1:8090/about", Code: "missing-title"}}}, nil
	default:
		return nil, status.Error(codes.NotFound, "report not found")
	}
}
