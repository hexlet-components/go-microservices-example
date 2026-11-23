package auditdemo

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// CertificateWarning is how long before expiry a certificate becomes a finding.
	CertificateWarning = 14 * 24 * time.Hour
	// SlowResponseLimit is the longest acceptable time to load the page.
	SlowResponseLimit = time.Second
)

// Page holds what the prepared loader measured for the requested page.
type Page struct {
	URL               string
	Header            http.Header
	Elapsed           time.Duration
	CertificateExpiry time.Time
	CheckedAt         time.Time
}

type Rule func(Page) []Finding

func ExpiringCertificate(p Page) []Finding {
	if !p.CertificateExpiry.IsZero() && p.CertificateExpiry.Sub(p.CheckedAt) < CertificateWarning {
		return []Finding{{PageURL: p.URL, Code: "tls-expires-soon"}}
	}
	return nil
}

func SlowResponse(p Page) []Finding {
	if p.Elapsed > SlowResponseLimit {
		return []Finding{{PageURL: p.URL, Code: "slow-response"}}
	}
	return nil
}

func MissingContentSecurityPolicy(p Page) []Finding {
	if strings.TrimSpace(p.Header.Get("Content-Security-Policy")) == "" {
		return []Finding{{PageURL: p.URL, Code: "missing-content-security-policy"}}
	}
	return nil
}

func MissingContentTypeOptions(p Page) []Finding {
	if !strings.EqualFold(strings.TrimSpace(p.Header.Get("X-Content-Type-Options")), "nosniff") {
		return []Finding{{PageURL: p.URL, Code: "missing-content-type-options"}}
	}
	return nil
}

func MissingReferrerPolicy(p Page) []Finding {
	if strings.TrimSpace(p.Header.Get("Referrer-Policy")) == "" {
		return []Finding{{PageURL: p.URL, Code: "missing-referrer-policy"}}
	}
	return nil
}

type Checker struct {
	Origin *url.URL
	Client *http.Client
	Rules  []Rule
	Now    func() time.Time
}

func NewChecker(origin string) (*Checker, error) {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, ErrInvalid
	}
	u.Path = ""
	u.RawQuery = ""
	u.Fragment = ""
	c := &Checker{
		Origin: u,
		Rules:  []Rule{ExpiringCertificate, SlowResponse, MissingContentSecurityPolicy, MissingContentTypeOptions},
		Now:    time.Now,
	}
	c.Client = &http.Client{
		Timeout:   2 * time.Second,
		Transport: &http.Transport{Proxy: nil},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 || !c.Allowed(req.URL) {
				return fmt.Errorf("redirect outside fixture")
			}
			return nil
		},
	}
	return c, nil
}

func (c *Checker) Allowed(u *url.URL) bool {
	return u.User == nil && u.Scheme == c.Origin.Scheme && u.Host == c.Origin.Host
}

// Check loads exactly one page and applies the rules to the response.
func (c *Checker) Check(ctx context.Context, target string) ([]Finding, error) {
	u, err := url.Parse(target)
	if err != nil || !c.Allowed(u) {
		return nil, ErrInvalid
	}
	u.Fragment = ""
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	started := time.Now()
	response, err := c.Client.Do(req)
	if err == nil {
		const maxBytes = 1 << 20
		_, err = io.Copy(io.Discard, io.LimitReader(response.Body, maxBytes))
		response.Body.Close()
	}
	elapsed := time.Since(started)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// A failed bounded load of the requested page is an audit finding.
		return []Finding{{PageURL: u.String(), Code: "unavailable"}}, nil
	}
	if response.StatusCode >= 500 {
		return []Finding{{PageURL: u.String(), Code: "unavailable"}}, nil
	}
	page := Page{URL: u.String(), Header: response.Header, Elapsed: elapsed, CheckedAt: c.Now()}
	if response.TLS != nil && len(response.TLS.PeerCertificates) > 0 {
		page.CertificateExpiry = response.TLS.PeerCertificates[0].NotAfter
	}
	findings := make([]Finding, 0)
	for _, rule := range c.Rules {
		findings = append(findings, rule(page)...)
	}
	return findings, nil
}

// Fixture serves the local page. It sends a title but no security headers.
func Fixture() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, `<!doctype html><html><head><title>Учебный сайт</title></head><body>Локальная страница для аудита</body></html>`)
	})
	return mux
}
