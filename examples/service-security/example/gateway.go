package security

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "hexlet.local/servicesecurity/reports/v1"
)

func UserToken(secret string, ownerID int64) (string, error) {
	if len(secret) < 32 || ownerID <= 0 {
		return "", errors.New("valid demo user and JWT_SECRET of at least 32 bytes are required")
	}
	claims := jwt.RegisteredClaims{Issuer: "audit-demo", Audience: jwt.ClaimStrings{"audit-demo"}, Subject: strconv.FormatInt(ownerID, 10), ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
}

func Gateway(client pb.ReportsServiceClient, secret string) http.Handler {
	owners := map[string]int64{"a-1": 1, "a-2": 2}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /audits/{audit_id}/report", func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		token, err := jwt.ParseWithClaims(strings.TrimPrefix(header, "Bearer "), &jwt.RegisteredClaims{}, func(*jwt.Token) (any, error) { return []byte(secret), nil }, jwt.WithValidMethods([]string{"HS256"}), jwt.WithIssuer("audit-demo"), jwt.WithAudience("audit-demo"), jwt.WithExpirationRequired())
		if err != nil || !token.Valid {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		userID, err := strconv.ParseInt(token.Claims.(*jwt.RegisteredClaims).Subject, 10, 64)
		if err != nil || userID <= 0 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		auditID := r.PathValue("audit_id")
		ownerID, exists := owners[auditID]
		if !exists || ownerID != userID {
			http.Error(w, "audit not found", http.StatusNotFound)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		report, err := client.GetReport(ctx, &pb.GetReportRequest{AuditId: auditID})
		if err != nil {
			code := http.StatusServiceUnavailable
			if status.Code(err) == codes.NotFound {
				code = http.StatusNotFound
			}
			http.Error(w, http.StatusText(code), code)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		findings := make([]map[string]string, 0, len(report.Findings))
		for _, f := range report.Findings {
			findings = append(findings, map[string]string{"page_url": f.PageUrl, "code": f.Code})
		}
		json.NewEncoder(w).Encode(map[string]any{"audit_id": report.AuditId, "ready": report.Ready, "findings": findings})
	})
	return mux
}
