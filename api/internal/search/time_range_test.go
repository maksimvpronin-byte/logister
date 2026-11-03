package search

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSearchSendsExactMillisecondBounds(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		for _, nanos := range []int{0, 123456789} {
			t.Run(fmt.Sprintf("snapshot=%v/nanos=%d", snapshot, nanos), func(t *testing.T) {
				from := time.Date(2026, 11, 3, 3, 0, 0, nanos, time.FixedZone("test", 3*3600))
				to := from.Add(time.Hour)
				searched := false
				backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if strings.HasSuffix(r.URL.Path, "/_mapping") {
						fmt.Fprint(w, `{"logs":{"mappings":{"_meta":{"ready":true}}}}`)
						return
					}
					if strings.Contains(r.URL.Path, "point_in_time") {
						fmt.Fprint(w, `{"pit_id":"snapshot"}`)
						return
					}
					var body struct {
						Query struct {
							Bool struct {
								Must []struct {
									Range map[string]struct {
										From   json.Number `json:"gte"`
										To     json.Number `json:"lte"`
										Format string      `json:"format"`
									} `json:"range"`
								} `json:"must"`
							} `json:"bool"`
						} `json:"query"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if len(body.Query.Bool.Must) < 2 {
						t.Fatal("missing time filter")
					}
					bounds := body.Query.Bool.Must[1].Range["@timestamp"]
					start, errStart := bounds.From.Int64()
					end, errEnd := bounds.To.Int64()
					if errStart != nil || errEnd != nil || bounds.Format != "epoch_millis" || start != from.UnixMilli() || end != to.UnixMilli() {
						t.Errorf("time filter must carry exact epoch milliseconds: %+v", bounds)
					}
					searched = true
					fmt.Fprint(w, `{"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`)
				}))
				defer backend.Close()
				client, err := NewClient(backend.URL)
				if err != nil {
					t.Fatal(err)
				}
				params := SearchParams{From: from, To: to, Size: 10}
				if snapshot {
					_, err = client.SearchPage(context.Background(), "logs", params, "")
				} else {
					_, err = client.Search(context.Background(), "logs", params)
				}
				if err != nil || !searched {
					t.Fatalf("searched=%v err=%v", searched, err)
				}
			})
		}
	}
}
