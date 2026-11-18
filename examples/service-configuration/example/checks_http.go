package auditdemo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

func ChecksAPI(service Checks) http.Handler {
	mux := http.NewServeMux()
	failure := func(w http.ResponseWriter, err error) {
		if errors.Is(err, ErrNotReady) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "result not ready"})
			return
		}
		writeError(w, err)
	}
	mux.HandleFunc("POST /checks", func(w http.ResponseWriter, r *http.Request) {
		var audit Audit
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&audit); err != nil {
			failure(w, ErrInvalid)
			return
		}
		if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			failure(w, ErrInvalid)
			return
		}
		if err := service.Start(r.Context(), audit); err != nil {
			failure(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /checks/{audit_id}/result", func(w http.ResponseWriter, r *http.Request) {
		result, err := service.Result(r.Context(), r.PathValue("audit_id"))
		if err != nil {
			failure(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	mux.HandleFunc("GET /checks/{audit_id}/state", func(w http.ResponseWriter, r *http.Request) {
		state, err := service.State(r.Context(), r.PathValue("audit_id"))
		if err != nil {
			failure(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]CheckState{"state": state})
	})
	return mux
}

type HTTPChecks struct {
	BaseURL string
	Client  *http.Client
}

func (c HTTPChecks) call(ctx context.Context, method, path string, body any, output any) error {
	var input io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		input = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+path, input)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := c.Client.Do(req)
	if err != nil {
		return fmt.Errorf("checks request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		switch response.StatusCode {
		case http.StatusNotFound:
			return ErrNotFound
		case http.StatusBadRequest:
			return ErrInvalid
		case http.StatusConflict:
			if method == http.MethodGet {
				return ErrNotReady
			}
			return ErrConflict
		default:
			return fmt.Errorf("checks status %d", response.StatusCode)
		}
	}
	if output == nil {
		_, err := io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return err
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return fmt.Errorf("checks response too large")
	}
	return json.Unmarshal(data, output)
}
func (c HTTPChecks) Start(ctx context.Context, audit Audit) error {
	return c.call(ctx, http.MethodPost, "/checks", audit, nil)
}
func (c HTTPChecks) Result(ctx context.Context, id string) (Result, error) {
	var result Result
	err := c.call(ctx, http.MethodGet, "/checks/"+url.PathEscape(id)+"/result", nil, &result)
	return result, err
}
func (c HTTPChecks) State(ctx context.Context, id string) (CheckState, error) {
	var output struct {
		State CheckState `json:"state"`
	}
	if err := c.call(ctx, http.MethodGet, "/checks/"+url.PathEscape(id)+"/state", nil, &output); err != nil {
		return "", err
	}
	if output.State != CheckRunning && output.State != CheckDone {
		return "", fmt.Errorf("unknown check state %q", output.State)
	}
	return output.State, nil
}
