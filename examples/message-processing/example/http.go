package auditdemo

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func Token(secret string, ownerID int64) (string, error) {
	claims := jwt.RegisteredClaims{
		Issuer: "audit-demo", Subject: strconv.FormatInt(ownerID, 10), Audience: jwt.ClaimStrings{"audit-demo"},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
}

func API(service Service, secret string) http.Handler {
	mux := http.NewServeMux()
	authenticated := func(next func(http.ResponseWriter, *http.Request, int64)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			const prefix = "Bearer "
			auth := r.Header.Get("Authorization")
			if len(auth) < len(prefix) || auth[:len(prefix)] != prefix {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			token, err := jwt.ParseWithClaims(auth[len(prefix):], &jwt.RegisteredClaims{}, func(t *jwt.Token) (any, error) { return []byte(secret), nil },
				jwt.WithValidMethods([]string{"HS256"}), jwt.WithIssuer("audit-demo"), jwt.WithAudience("audit-demo"), jwt.WithExpirationRequired())
			if err != nil || !token.Valid {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			claims := token.Claims.(*jwt.RegisteredClaims)
			ownerID, err := strconv.ParseInt(claims.Subject, 10, 64)
			if err != nil || ownerID <= 0 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next(w, r, ownerID)
		}
	}
	mux.HandleFunc("POST /audits", authenticated(func(w http.ResponseWriter, r *http.Request, ownerID int64) {
		var body struct {
			URL string `json:"url"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&body); err != nil {
			writeError(w, ErrInvalid)
			return
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			writeError(w, ErrInvalid)
			return
		}
		audit, err := service.Create(r.Context(), ownerID, r.Header.Get("Idempotency-Key"), body.URL)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"audit_id": audit.ID})
	}))
	mux.HandleFunc("GET /audits/{audit_id}/report", authenticated(func(w http.ResponseWriter, r *http.Request, ownerID int64) {
		report, err := service.Report(r.Context(), ownerID, r.PathValue("audit_id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, report)
	}))
	return mux
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Error("response_write_failed", "error", err)
	}
}

func writeError(w http.ResponseWriter, err error) {
	code := http.StatusServiceUnavailable
	switch {
	case errors.Is(err, ErrInvalid):
		code = http.StatusBadRequest
	case errors.Is(err, ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, ErrConflict):
		code = http.StatusConflict
	}
	if code == http.StatusServiceUnavailable {
		slog.Error("audit_unfinished", "error", err)
	}
	writeJSON(w, code, map[string]string{"error": fmt.Sprintf("%d %s", code, http.StatusText(code))})
}
