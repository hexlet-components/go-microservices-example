package security

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	pb "hexlet.local/servicesecurity/reports/v1"
)

const apiToken = "test-api-service-token-32-characters"
const checksToken = "test-checks-service-token-32-characters"
const userSecret = "test-user-jwt-secret-32-characters"

func secureServer(t *testing.T) (string, *bytes.Buffer) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	output := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(output, nil))
	server, err := NewServer("certs/localhost.crt", "certs/localhost-test.key", []ServiceIdentity{{Token: apiToken, CanReadReports: true}, {Token: checksToken, CanReadReports: false}}, logger)
	if err != nil {
		t.Fatal(err)
	}
	go server.Serve(listener)
	t.Cleanup(server.GracefulStop)
	return listener.Addr().String(), output
}

func secureClient(t *testing.T, address, token string) pb.ReportsServiceClient {
	t.Helper()
	connection, err := Dial(address, "certs/ca.crt", "localhost", token)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return pb.NewReportsServiceClient(connection)
}

func TestServiceAuthenticationAndRights(t *testing.T) {
	address, output := secureServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client := secureClient(t, address, apiToken)
	report, err := client.GetReport(ctx, &pb.GetReportRequest{AuditId: "a-1"})
	if err != nil || !report.GetReady() {
		t.Fatalf("report=%v error=%v", report, err)
	}
	for _, row := range []struct {
		token string
		want  codes.Code
	}{{"unknown-service-token", codes.Unauthenticated}, {checksToken, codes.PermissionDenied}} {
		caller := secureClient(t, address, row.token)
		untrusted := metadata.AppendToOutgoingContext(ctx, "user_id", "1")
		_, err := caller.GetReport(untrusted, &pb.GetReportRequest{AuditId: "a-1"})
		if status.Code(err) != row.want {
			t.Errorf("status=%s want=%s", status.Code(err), row.want)
		}
	}
	logs := output.String()
	if !strings.Contains(logs, "Unauthenticated") || !strings.Contains(logs, "PermissionDenied") {
		t.Fatalf("missing denied-call diagnostics: %s", logs)
	}
	if strings.Contains(logs, apiToken) || strings.Contains(logs, checksToken) || strings.Contains(logs, "unknown-service-token") {
		t.Fatal("service token leaked to diagnostics")
	}
}

func TestTLSChecksServerCertificate(t *testing.T) {
	address, _ := secureServer(t)
	connection, err := Dial(address, "certs/ca.crt", "wrong-server.invalid", apiToken)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := pb.NewReportsServiceClient(connection).GetReport(ctx, &pb.GetReportRequest{AuditId: "a-1"}); err == nil {
		t.Fatal("incorrect server name accepted")
	}
	untrusted, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: x509.NewCertPool(), ServerName: "localhost", MinVersion: tls.VersionTLS12})), grpc.WithPerRPCCredentials(TokenCredentials{Token: apiToken}))
	if err != nil {
		t.Fatal(err)
	}
	defer untrusted.Close()
	if _, err := pb.NewReportsServiceClient(untrusted).GetReport(ctx, &pb.GetReportRequest{AuditId: "a-1"}); err == nil {
		t.Fatal("untrusted CA accepted")
	}
}

func TestCredentialsRequireTLS(t *testing.T) {
	address, _ := secureServer(t)
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(TokenCredentials{Token: apiToken}))
	if err == nil {
		connection.Close()
		t.Fatal("secure token credentials accepted insecure transport")
	}
}

func TestGatewayVerifiesUserAndOwnership(t *testing.T) {
	address, _ := secureServer(t)
	client := secureClient(t, address, apiToken)
	gateway := httptest.NewTLSServer(Gateway(client, userSecret))
	defer gateway.Close()
	token, err := UserToken(userSecret, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		id, token string
		want      int
	}{{"a-1", token, http.StatusOK}, {"a-2", token, http.StatusNotFound}, {"a-1", "unverified-token", http.StatusUnauthorized}} {
		request, _ := http.NewRequest(http.MethodGet, gateway.URL+"/audits/"+row.id+"/report", nil)
		request.Header.Set("Authorization", "Bearer "+row.token)
		request.Header.Set("X-User-Id", "2")
		response, err := gateway.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != row.want {
			t.Errorf("id=%s status=%d want=%d", row.id, response.StatusCode, row.want)
		}
	}
}
