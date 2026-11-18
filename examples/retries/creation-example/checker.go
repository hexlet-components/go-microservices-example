package auditdemo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/html"
)

// Page and the parser belong to the prepared loader.
type Page struct {
	URL            string
	Title          string
	HasDescription bool
	Links          []string
}

type Rule func(Page) []Finding

func MissingTitle(p Page) []Finding {
	if p.Title == "" {
		return []Finding{{PageURL: p.URL, Code: "missing-title"}}
	}
	return nil
}

func MissingDescription(p Page) []Finding {
	if !p.HasDescription {
		return []Finding{{PageURL: p.URL, Code: "missing-description"}}
	}
	return nil
}

type Checker struct {
	Origin   *url.URL
	Client   *http.Client
	Rules    []Rule
	MaxPages int
}

func NewChecker(origin string) (*Checker, error) {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, ErrInvalid
	}
	u.Path = ""
	u.RawQuery = ""
	u.Fragment = ""
	c := &Checker{Origin: u, Rules: []Rule{MissingTitle}, MaxPages: 5}
	c.Client = &http.Client{
		Timeout:   500 * time.Millisecond,
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

func (c *Checker) Check(ctx context.Context, target string) ([]Finding, error) {
	start, err := url.Parse(target)
	if err != nil || !c.Allowed(start) {
		return nil, ErrInvalid
	}
	queue := []*url.URL{start}
	seen := map[string]bool{}
	findings := make([]Finding, 0)
	for len(queue) > 0 && len(seen) < c.MaxPages {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		u := queue[0]
		queue = queue[1:]
		u.Fragment = ""
		if seen[u.String()] {
			continue
		}
		seen[u.String()] = true
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		response, err := c.Client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			// A failed bounded fetch of an allowed page is an audit finding.
			findings = append(findings, Finding{PageURL: u.String(), Code: "load-error"})
			continue
		}
		page, readErr := readPage(u.String(), response.Body)
		response.Body.Close()
		if readErr != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			findings = append(findings, Finding{PageURL: u.String(), Code: "load-error"})
			continue
		}
		if response.StatusCode >= 400 {
			findings = append(findings, Finding{PageURL: u.String(), Code: fmt.Sprintf("http-%d", response.StatusCode)})
			continue
		}
		for _, rule := range c.Rules {
			findings = append(findings, rule(page)...)
		}
		for _, link := range page.Links {
			relative, err := url.Parse(link)
			if err != nil {
				continue
			}
			next := u.ResolveReference(relative)
			if c.Allowed(next) {
				queue = append(queue, next)
			}
		}
	}
	return findings, nil
}

func readPage(address string, body io.Reader) (Page, error) {
	const maxBytes = 1 << 20
	bytes, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
	if err != nil {
		return Page{}, err
	}
	if len(bytes) > maxBytes {
		return Page{}, errors.New("page exceeds fixture size")
	}
	document, err := html.Parse(strings.NewReader(string(bytes)))
	if err != nil {
		return Page{}, err
	}
	page := Page{URL: address}
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			switch n.Data {
			case "title":
				if n.FirstChild != nil {
					page.Title = strings.TrimSpace(n.FirstChild.Data)
				}
			case "a":
				for _, a := range n.Attr {
					if a.Key == "href" {
						page.Links = append(page.Links, a.Val)
					}
				}
			case "meta":
				name, content := "", ""
				for _, a := range n.Attr {
					if a.Key == "name" {
						name = a.Val
					}
					if a.Key == "content" {
						content = a.Val
					}
				}
				if strings.EqualFold(name, "description") && strings.TrimSpace(content) != "" {
					page.HasDescription = true
				}
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(document)
	return page, nil
}

func Fixture() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, `<!doctype html><html><head><title>Учебный сайт</title><meta name="description" content="Локальный сайт для аудита"></head><body><a href="/about">О сайте</a><a href="/missing">Отсутствующая страница</a></body></html>`)
	})
	mux.HandleFunc("GET /about", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, `<!doctype html><html><head></head><body>О сайте</body></html>`)
	})
	return mux
}
