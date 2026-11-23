package auditdemo

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func codes(findings []Finding) map[string]int {
	result := map[string]int{}
	for _, f := range findings {
		result[f.Code]++
	}
	return result
}

func TestFixtureRules(t *testing.T) {
	site := httptest.NewServer(Fixture())
	defer site.Close()
	checker, err := NewChecker(site.URL)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := checker.Check(context.Background(), site.URL+"/")
	if err != nil {
		t.Fatal(err)
	}
	found := codes(findings)
	if len(findings) != 2 || found["missing-content-security-policy"] != 1 || found["missing-content-type-options"] != 1 {
		t.Fatalf("findings=%+v", findings)
	}
	checker.Rules = append(checker.Rules, MissingReferrerPolicy)
	findings, err = checker.Check(context.Background(), site.URL+"/")
	if err != nil || len(findings) != 3 || codes(findings)["missing-referrer-policy"] != 1 {
		t.Fatalf("findings=%+v error=%v", findings, err)
	}
	if _, err := checker.Check(context.Background(), "http://another.invalid/"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("external origin error=%v", err)
	}
}

func TestRulesUsePreparedPage(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	secure := http.Header{}
	secure.Set("Content-Security-Policy", "default-src 'self'")
	secure.Set("X-Content-Type-Options", "nosniff")
	secure.Set("Referrer-Policy", "no-referrer")
	cases := []struct {
		name string
		rule Rule
		page Page
		want string
	}{
		{"certificate expires soon", ExpiringCertificate, Page{CertificateExpiry: now.Add(72 * time.Hour), CheckedAt: now}, "tls-expires-soon"},
		{"certificate valid long enough", ExpiringCertificate, Page{CertificateExpiry: now.Add(90 * 24 * time.Hour), CheckedAt: now}, ""},
		{"plain http page", ExpiringCertificate, Page{CheckedAt: now}, ""},
		{"slow response", SlowResponse, Page{Elapsed: 1500 * time.Millisecond}, "slow-response"},
		{"fast response", SlowResponse, Page{Elapsed: 20 * time.Millisecond}, ""},
		{"no policy", MissingContentSecurityPolicy, Page{Header: http.Header{}}, "missing-content-security-policy"},
		{"policy present", MissingContentSecurityPolicy, Page{Header: secure}, ""},
		{"no nosniff", MissingContentTypeOptions, Page{Header: http.Header{}}, "missing-content-type-options"},
		{"nosniff present", MissingContentTypeOptions, Page{Header: secure}, ""},
		{"no referrer policy", MissingReferrerPolicy, Page{Header: http.Header{}}, "missing-referrer-policy"},
		{"referrer policy present", MissingReferrerPolicy, Page{Header: secure}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.page.URL = "http://fixture/"
			findings := tc.rule(tc.page)
			if tc.want == "" && len(findings) != 0 {
				t.Fatalf("findings=%+v", findings)
			}
			if tc.want != "" && (len(findings) != 1 || findings[0].Code != tc.want || findings[0].PageURL != "http://fixture/") {
				t.Fatalf("findings=%+v", findings)
			}
		})
	}
}

func TestCertificateExpiryComesFromResponse(t *testing.T) {
	site := httptest.NewTLSServer(Fixture())
	defer site.Close()
	checker, err := NewChecker(site.URL)
	if err != nil {
		t.Fatal(err)
	}
	checker.Client.Transport = site.Client().Transport
	checker.Rules = []Rule{ExpiringCertificate}
	findings, err := checker.Check(context.Background(), site.URL+"/")
	if err != nil || len(findings) != 0 {
		t.Fatalf("findings=%+v error=%v", findings, err)
	}
	expiry := site.Certificate().NotAfter
	checker.Now = func() time.Time { return expiry.Add(-72 * time.Hour) }
	findings, err = checker.Check(context.Background(), site.URL+"/")
	if err != nil || len(findings) != 1 || findings[0].Code != "tls-expires-soon" {
		t.Fatalf("findings=%+v error=%v", findings, err)
	}
}

func TestStatusCodes(t *testing.T) {
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		switch r.URL.Path {
		case "/gone":
			http.NotFound(w, r)
		case "/down":
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer site.Close()
	checker, _ := NewChecker(site.URL)
	findings, err := checker.Check(context.Background(), site.URL+"/gone")
	if err != nil || len(findings) != 0 {
		t.Fatalf("client error page findings=%+v error=%v", findings, err)
	}
	findings, err = checker.Check(context.Background(), site.URL+"/down")
	if err != nil || len(findings) != 1 || findings[0].Code != "unavailable" {
		t.Fatalf("server error findings=%+v error=%v", findings, err)
	}
}

func TestFetchFailuresAreFindings(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
		defer site.Close()
		checker, _ := NewChecker(site.URL)
		checker.Client.Timeout = 20 * time.Millisecond
		findings, err := checker.Check(context.Background(), site.URL)
		if err != nil || len(findings) != 1 || findings[0].Code != "unavailable" {
			t.Fatalf("findings=%v error=%v", findings, err)
		}
	})
	t.Run("dns", func(t *testing.T) {
		checker, _ := NewChecker("http://fixture.invalid")
		checker.Client.Transport = &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
			return nil, &net.DNSError{Err: "no such host", Name: "fixture.invalid", IsNotFound: true}
		}}
		findings, err := checker.Check(context.Background(), "http://fixture.invalid/")
		if err != nil || len(findings) != 1 || findings[0].Code != "unavailable" {
			t.Fatalf("findings=%v error=%v", findings, err)
		}
	})
	t.Run("untrusted certificate", func(t *testing.T) {
		site := httptest.NewTLSServer(Fixture())
		defer site.Close()
		checker, _ := NewChecker(site.URL)
		checker.Client.Transport = &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
		findings, err := checker.Check(context.Background(), site.URL)
		if err != nil || len(findings) != 1 || findings[0].Code != "unavailable" {
			t.Fatalf("findings=%v error=%v", findings, err)
		}
	})
}

func TestCancellationLeavesWorkUnfinished(t *testing.T) {
	site := httptest.NewServer(Fixture())
	defer site.Close()
	checker, _ := NewChecker(site.URL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := checker.Check(ctx, site.URL); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}
