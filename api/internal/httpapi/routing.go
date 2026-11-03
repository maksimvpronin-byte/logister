package httpapi

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/fieldpath"
)

func routeIngest(req ingestRequest, streams []db.Stream) (ingestRequest, error) {
	decision, err := decideRouting(req, streams, false)
	if err != nil {
		return req, err
	}
	req.Stream = decision.Stream
	return req, nil
}

type routingRuleResult struct {
	db.RoutingRule
	Actual  string `json:"actual"`
	Exists  bool   `json:"exists"`
	Matched bool   `json:"matched"`
}
type routingCandidate struct {
	Name     string              `json:"name"`
	Title    string              `json:"title"`
	Enabled  bool                `json:"enabled"`
	Priority int                 `json:"priority"`
	Match    string              `json:"match"`
	Matched  bool                `json:"matched"`
	Selected bool                `json:"selected"`
	Rules    []routingRuleResult `json:"rules"`
}
type routingDecision struct {
	Stream     string             `json:"stream"`
	Reason     string             `json:"reason"`
	Candidates []routingCandidate `json:"candidates"`
}

func decideRouting(req ingestRequest, streams []db.Stream, explain bool) (routingDecision, error) {
	decision := routingDecision{Stream: "all", Reason: "fallback", Candidates: []routingCandidate{}}
	if req.Stream != "" {
		for _, stream := range streams {
			if stream.Name == req.Stream {
				decision.Stream = req.Stream
				decision.Reason = "explicit"
				return decision, nil
			}
		}
		return decision, fmt.Errorf("unknown stream %q; create it in Streams settings first", req.Stream)
	}
	ordered := append([]db.Stream(nil), streams...)
	root := routingRoot(req)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Routing.Priority != ordered[j].Routing.Priority {
			return ordered[i].Routing.Priority < ordered[j].Routing.Priority
		}
		return ordered[i].Name < ordered[j].Name
	})
	for _, stream := range ordered {
		r := stream.Routing
		if stream.Name == "all" {
			continue
		}
		matched := r.Match == "all"
		candidate := routingCandidate{Name: stream.Name, Title: stream.Title, Enabled: r.Enabled, Priority: r.Priority, Match: r.Match, Rules: []routingRuleResult{}}
		for _, rule := range r.Rules {
			values, _ := fieldpath.Values(root, rule.Field)
			exists := len(values) > 0
			value := ""
			if explain {
				if len(values) == 1 {
					value, _ = routingScalar(values[0])
				} else if exists {
					raw, _ := json.Marshal(values)
					value = string(raw)
				}
				value = trimText(value, 8000)
			}
			hit := matchJSONValues(values, rule.Operator, rule.Value)
			if explain {
				candidate.Rules = append(candidate.Rules, routingRuleResult{RoutingRule: rule, Actual: value, Exists: exists, Matched: hit})
			}
			if r.Match == "any" {
				matched = matched || hit
			} else {
				matched = matched && hit
			}
		}
		matched = matched && r.Enabled && len(r.Rules) > 0
		candidate.Matched = matched
		if matched && decision.Reason == "fallback" {
			decision.Stream = stream.Name
			decision.Reason = "rule"
			candidate.Selected = true
			if !explain {
				return decision, nil
			}
		}
		if explain {
			decision.Candidates = append(decision.Candidates, candidate)
		}
	}
	return decision, nil
}

func routingValue(req ingestRequest, field string) (string, bool) {
	values, err := fieldpath.Values(routingRoot(req), field)
	if err != nil || len(values) == 0 {
		return "", false
	}
	if len(values) == 1 {
		return routingScalar(values[0])
	}
	raw, _ := json.Marshal(values)
	return string(raw), true
}
func routingRoot(req ingestRequest) map[string]any {
	if req.Lookup != nil {
		return req.Lookup
	}
	if req.Payload != nil {
		return req.Payload
	}
	root := map[string]any{}
	if req.Payload == nil {
		root = map[string]any{"source": req.Source, "host": req.Host, "level": req.Level, "facility": req.Facility, "message": req.Message, "fields": req.Fields}
	}
	for key, value := range req.Payload {
		root[key] = value
	}
	for key, value := range req.Lookup {
		root[key] = value
	}
	return root
}
func matchJSONValues(values []any, operator, wanted string) bool {
	if operator == "exists" {
		return len(values) > 0
	}
	if operator == "not_exists" {
		return len(values) == 0
	}
	for _, value := range values {
		text, _ := routingScalar(value)
		if operator == "equals" && text == wanted || operator == "contains" && strings.Contains(text, wanted) || operator == "starts_with" && strings.HasPrefix(text, wanted) {
			return true
		}
		if operator == "equals" {
			switch value.(type) {
			case map[string]any, []any:
				var comparison any
				decoder := json.NewDecoder(strings.NewReader(wanted))
				decoder.UseNumber()
				if decoder.Decode(&comparison) == nil {
					raw, _ := json.Marshal(comparison)
					if string(raw) == text {
						return true
					}
				}
			}
		}
		if items, ok := value.([]any); ok && matchJSONValues(items, operator, wanted) {
			return true
		}
	}
	return false
}
func routingScalar(value any) (string, bool) {
	if text, ok := value.(string); ok {
		return text, true
	}
	raw, err := json.Marshal(value)
	return string(raw), err == nil
}
