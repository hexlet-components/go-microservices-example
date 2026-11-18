package release

import (
	"encoding/json"
	"errors"
)

type Finding struct {
	PageURL string `json:"page_url"`
	Code    string `json:"code"`
}
type EventV1 struct {
	EventID  string    `json:"event_id"`
	AuditID  string    `json:"audit_id"`
	ResultID string    `json:"result_id"`
	Findings []Finding `json:"findings"`
}
type EventV2 struct {
	EventV1
	RuleVersion *string `json:"rule_version,omitempty"`
}
type Report struct {
	AuditID  string    `json:"audit_id"`
	Ready    bool      `json:"ready"`
	Findings []Finding `json:"findings"`
}

func ConsumeV1(data []byte) (Report, error) {
	var event EventV1
	if err := json.Unmarshal(data, &event); err != nil {
		return Report{}, err
	}
	if event.EventID == "" || event.AuditID == "" || event.ResultID == "" {
		return Report{}, errors.New("missing event identity")
	}
	for _, f := range event.Findings {
		if f.Code != "missing-title" && f.Code != "http-404" {
			return Report{}, errors.New("unknown finding")
		}
	}
	return Report{AuditID: event.AuditID, Ready: true, Findings: event.Findings}, nil
}
func ConsumeV2(data []byte) (Report, error) {
	var event EventV2
	if err := json.Unmarshal(data, &event); err != nil {
		return Report{}, err
	}
	return ConsumeV1(data)
}
