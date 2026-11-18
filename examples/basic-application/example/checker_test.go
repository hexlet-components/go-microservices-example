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
	codes := map[string]int{}
	for _, f := range findings {
		codes[f.Code]++
	}
	if len(findings) != 2 || codes["missing-title"] != 1 || codes["http-404"] != 1 {
		t.Fatalf("findings=%+v", findings)
	}
	checker.Rules = append(checker.Rules, MissingDescription)
	findings, err = checker.Check(context.Background(), site.URL+"/")
	if err != nil || len(findings) != 3 {
		t.Fatalf("findings=%+v error=%v", findings, err)
	}
	if _, err := checker.Check(context.Background(), "http://another.invalid/"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("external origin error=%v", err)
	}
}

func TestFetchFailuresAreFindings(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
		defer site.Close()
		checker, _ := NewChecker(site.URL)
		checker.Client.Timeout = 20 * time.Millisecond
		findings, err := checker.Check(context.Background(), site.URL)
		if err != nil || len(findings) != 1 || findings[0].Code != "load-error" {
			t.Fatalf("findings=%v error=%v", findings, err)
		}
	})
	t.Run("dns", func(t *testing.T) {
		checker, _ := NewChecker("http://fixture.invalid")
		checker.Client.Transport = &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
			return nil, &net.DNSError{Err: "no such host", Name: "fixture.invalid", IsNotFound: true}
		}}
		findings, err := checker.Check(context.Background(), "http://fixture.invalid/")
		if err != nil || len(findings) != 1 || findings[0].Code != "load-error" {
			t.Fatalf("findings=%v error=%v", findings, err)
		}
	})
	t.Run("tls", func(t *testing.T) {
		site := httptest.NewTLSServer(Fixture())
		defer site.Close()
		checker, _ := NewChecker(site.URL)
		checker.Client.Transport = &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
		findings, err := checker.Check(context.Background(), site.URL)
		if err != nil || len(findings) != 1 || findings[0].Code != "load-error" {
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
