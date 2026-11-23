package compatdemo

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
	AuditID  string
	Ready    bool
	Findings []Finding
}

func report(e EventV1) (Report, error) {
	if e.EventID == "" || e.AuditID == "" || e.ResultID == "" {
		return Report{}, errors.New("missing identity")
	}
	for _, f := range e.Findings {
		if f.Code != "missing-content-security-policy" && f.Code != "missing-content-type-options" {
			return Report{}, errors.New("unknown finding code")
		}
	}
	return Report{AuditID: e.AuditID, Ready: true, Findings: e.Findings}, nil
}
func ConsumeV1(data []byte) (Report, error) {
	var e EventV1
	if err := json.Unmarshal(data, &e); err != nil {
		return Report{}, err
	}
	return report(e)
}
func ConsumeV2(data []byte) (Report, error) {
	var e EventV2
	if err := json.Unmarshal(data, &e); err != nil {
		return Report{}, err
	}
	return report(e.EventV1)
}
