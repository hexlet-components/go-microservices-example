package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	security "hexlet.local/servicesecurity"
	pb "hexlet.local/servicesecurity/reports/v1"
)

func main() {
	serverMode := flag.Bool("server", false, "serve reports over TLS")
	apiMode := flag.Bool("api", false, "serve the user API over HTTPS on localhost")
	tokenMode := flag.Bool("token", false, "print a fixture user token")
	user := flag.Int64("user", 1, "fixture user ID")
	auditID := flag.String("audit", "a-1", "audit ID")
	flag.Parse()
	secret := os.Getenv("JWT_SECRET")
	if *tokenMode {
		token, err := security.UserToken(secret, *user)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(token)
		return
	}
	if *serverMode {
		listener, err := net.Listen("tcp", "127.0.0.1:9091")
		if err != nil {
			log.Fatal(err)
		}
		identities := []security.ServiceIdentity{{Token: os.Getenv("API_SERVICE_TOKEN"), CanReadReports: true}, {Token: os.Getenv("CHECKS_SERVICE_TOKEN"), CanReadReports: false}}
		server, err := security.NewServer("certs/localhost.crt", "certs/localhost-test.key", identities, slog.Default())
		if err != nil {
			log.Fatal(err)
		}
		log.Print("reports TLS ready on 127.0.0.1:9091")
		log.Fatal(server.Serve(listener))
	}
	serviceToken := os.Getenv("SERVICE_TOKEN")
	if serviceToken == "" {
		serviceToken = os.Getenv("API_SERVICE_TOKEN")
	}
	connection, err := security.Dial("127.0.0.1:9091", "certs/ca.crt", "localhost", serviceToken)
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()
	client := pb.NewReportsServiceClient(connection)
	if *apiMode {
		if len(secret) < 32 {
			log.Fatal("JWT_SECRET must contain at least 32 bytes")
		}
		log.Print("user API HTTPS ready on 127.0.0.1:8443")
		log.Fatal((&http.Server{Addr: "127.0.0.1:8443", Handler: security.Gateway(client, secret), ReadHeaderTimeout: 2 * time.Second, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}).ListenAndServeTLS("certs/localhost.crt", "certs/localhost-test.key"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	report, err := client.GetReport(ctx, &pb.GetReportRequest{AuditId: *auditID})
	if err != nil {
		log.Fatal(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		log.Fatal(err)
	}
}
