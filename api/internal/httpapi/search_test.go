package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/logister/logister/api/internal/config"
	"github.com/logister/logister/api/internal/search"
)

func searchHandler(t *testing.T, backend http.HandlerFunc) http.Handler {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/_mapping") {
			_, _ = w.Write([]byte(`{"logs":{"mappings":{"_meta":{"ready":true}}}}`))
			return
		}
		if strings.Contains(r.URL.Path, "point_in_time") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"pit_id":"test-snapshot"}`))
			return
		}
		backend(w, r)
	}))
	t.Cleanup(upstream.Close)
	client, err := search.NewClient(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	return NewServer(config.Config{OpenSearchIndex: "logs"}, nil, client).Handler()
}

func searchCall(handler http.Handler, values url.Values) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/search?"+values.Encode(), nil))
	return recorder
}

func TestParseRelativeBounds(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{"15m", true}, {"1.5h", true}, {"7d", true}, {" 1H ", true}, {"106751d", true},
		{"0s", false}, {"-1h", false}, {"0d", false}, {"-1d", false}, {"bad", false},
		{"106752d", false}, {"9223372036854775807d", false}, {"", false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			d, err := parseRelative(tc.value)
			if (err == nil) != tc.valid || (tc.valid && d <= 0) {
				t.Fatalf("duration=%v error=%v valid=%v", d, err, tc.valid)
			}
		})
	}
}

func TestSearchRejectsInvalidParametersBeforeCallingStorage(t *testing.T) {
	var calls atomic.Int64
	handler := searchHandler(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`))
	})
	for _, query := range []string{
		"relative=0s", "relative=-1h", "relative=106752d", "relative=bad", "relative=",
		"from=invalid", "to=invalid", "from=", "to=", "from=2026-11-03T01:00:00Z&to=2026-11-03T00:00:00Z",
		"from=2026-11-03T00:00:00Z&to=2026-11-03T00:00:00Z", "relative=1h&from=2026-11-03T00:00:00Z",
		"size=abc", "size=0", "size=-1", "size=201", "size=999999999999999999999",
		"offset=-1", "offset=abc", "offset=10000", "offset=9901&size=100",
	} {
		t.Run(query, func(t *testing.T) {
			values, _ := url.ParseQuery(query)
			result := searchCall(handler, values)
			if result.Code != http.StatusBadRequest {
				t.Errorf("status=%d body=%s", result.Code, result.Body.String())
			}
		})
	}
	if calls.Load() != 0 {
		t.Errorf("invalid parameters reached storage %d times", calls.Load())
	}
}

func TestSearchReturnsExactCountAndPinnedRange(t *testing.T) {
	var requestBody map[string]any
	handler := searchHandler(t, func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"took":3,"hits":{"total":{"value":10001,"relation":"eq"},"hits":[]}}`))
	})
	result := searchCall(handler, url.Values{
		"q": {"level:error"}, "from": {"2026-11-03T00:00:00Z"}, "to": {"2026-11-03T01:00:00Z"},
		"size": {"100"}, "offset": {"100"},
	})
	if result.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", result.Code, result.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(result.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["total"] != float64(10001) || body["total_relation"] != "eq" {
		t.Errorf("incorrect count: %v", body)
	}
	if body["from"] != "2026-11-03T00:00:00Z" || body["to"] != "2026-11-03T01:00:00Z" {
		t.Errorf("lost range: %v", body)
	}
	if requestBody["track_total_hits"] != true || requestBody["from"] != float64(100) {
		t.Errorf("incorrect storage request: %v", requestBody)
	}
}

func TestSearchReturnsHelpfulErrorsWithoutStorageDetails(t *testing.T) {
	for _, tc := range []struct {
		status, expected int
		body             string
	}{
		{400, 400, `{"error":{"reason":"private-index-id: invalid syntax"}}`},
		{503, 503, `{"error":{"reason":"private-index-id: unavailable"}}`},
		{200, 503, `not-json-private-index-id`},
	} {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			handler := searchHandler(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			result := searchCall(handler, url.Values{"q": {"("}})
			if result.Code != tc.expected || strings.Contains(result.Body.String(), "private-index-id") {
				t.Errorf("status=%d body=%s", result.Code, result.Body.String())
			}
		})
	}
}

func TestSearchRelativeRangeAndLastValidPage(t *testing.T) {
	handler := searchHandler(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`))
	})
	result := searchCall(handler, url.Values{"relative": {"7d"}, "size": {"100"}, "offset": {"9900"}})
	if result.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", result.Code, result.Body.String())
	}
	var body struct{ From, To time.Time }
	if err := json.Unmarshal(result.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.To.Sub(body.From) != 7*24*time.Hour {
		t.Errorf("range=%s..%s", body.From, body.To)
	}
}
