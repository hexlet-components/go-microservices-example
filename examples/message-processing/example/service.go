package auditdemo

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"net/http"
	"net/url"
)

type Service struct {
	Store        Store
	Checker      *Checker
	Kafka        *kgo.Client
	ReportsURL   string
	CreatedTopic string
	HTTP         *http.Client
}

func (s Service) Create(ctx context.Context, owner int64, key, target string) (Audit, error) {
	u, err := url.Parse(target)
	if err != nil || !s.Checker.Allowed(u) || key == "" || len(key) > 128 {
		return Audit{}, ErrInvalid
	}
	a, err := s.Store.Create(ctx, owner, key, target)
	if err != nil {
		return Audit{}, err
	}
	// This sequential write deliberately leaves a publication gap for the outbox lesson.
	e := AuditCreated{EventID: "created-" + a.ID, AuditID: a.ID, URL: a.URL}
	topic := s.CreatedTopic
	if topic == "" {
		topic = "audit-created"
	}
	err = Publish(ctx, s.Kafka, topic, a.ID, e)
	return a, err
}
func (s Service) Report(ctx context.Context, owner int64, id string) (Report, error) {
	if err := s.Store.Owned(ctx, owner, id); err != nil {
		return Report{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.ReportsURL+"/reports/"+url.PathEscape(id), nil)
	if err != nil {
		return Report{}, err
	}
	response, err := s.HTTP.Do(request)
	if err != nil {
		return Report{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return Report{}, fmt.Errorf("reports status %d", response.StatusCode)
	}
	var report Report
	err = json.NewDecoder(response.Body).Decode(&report)
	return report, err
}
