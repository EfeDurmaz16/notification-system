package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSummaryNearestRank(t *testing.T) {
	n := summarize([]float64{9, 4, 6, 2})
	if n.N != 4 || n.P50 != 4 || n.P95 != 9 || n.P99 != 9 || n.Max != 9 {
		t.Errorf("%+v", n)
	}
}
func TestMeasureTerminalStates(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /stats", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"attempts":3,"accepted":2,"responseLost":1,"failures":1,"channels":{"sms":{"attempts":3,"peakPerSecond":3}}}`))
	})
	items := []item{{Index: 0, ID: "n0"}, {Index: 1, ID: "n1"}, {Index: 2, ID: "n2"}}
	mux.HandleFunc("POST /v1/batches", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Idempotency-Key") == "" {
			t.Error("missing idempotency key")
		}
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(batchResponse{"b0", items})
	})
	mux.HandleFunc("GET /v1/batches/b0", func(w http.ResponseWriter, _ *http.Request) {
		now := time.Now()
		_ = json.NewEncoder(w).Encode(batchResponse{"b0", []item{{Index: 0, ID: "n0", Status: "accepted", Updated: now}, {Index: 1, ID: "n1", Status: "failed", Updated: now}, {Index: 2, ID: "n2", Status: "unknown", Updated: now}}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := config{Scenario: "failures", API: srv.URL, Provider: srv.URL, Total: 3, Batch: 3, Concurrency: 2, Poll: time.Millisecond, Deadline: time.Second}
	h := harness{r: result{Config: c, APIStatuses: map[int]int{}, Statuses: map[string]int{}, Checks: map[string]bool{}}, client: srv.Client(), seen: map[string]string{}, runID: "test"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.run(ctx); err != nil {
		t.Fatal(err)
	}
	for n, ok := range h.r.Checks {
		if !ok {
			t.Error(n)
		}
	}
	if h.r.Observed != 3 || h.r.Statuses["unknown"] != 1 || h.r.TerminalLatency.N != 3 || h.r.StateLatency.N != 3 {
		t.Fatalf("%+v", h.r)
	}
}
func BenchmarkPercentiles1000(b *testing.B) {
	b.ReportAllocs()
	var data [1000]float64
	for b.Loop() {
		for i := range data {
			data[i] = float64((i * 109) % 1000)
		}
		_ = summarize(data[:])
	}
}

func TestPerIDReceiptAuditDetectsOffsettingLossAndDuplication(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stats/{id}", func(w http.ResponseWriter, r *http.Request) {
		values := map[string]int{"sent": 1, "missing": 0, "duplicate": 2}
		_ = json.NewEncoder(w).Encode(map[string]int{"attempts": values[r.PathValue("id")]})
	})
	s := httptest.NewServer(mux)
	defer s.Close()
	h := harness{r: result{Config: config{Provider: s.URL}}, client: s.Client(), seen: map[string]string{"sent": "accepted", "missing": "accepted", "duplicate": "accepted"}}
	a := h.audit(t.Context())
	if a.Checked != 3 || a.Missing != 1 || a.Repeated != 1 || a.ReadErrors != 0 {
		t.Fatalf("audit: %+v", a)
	}
}
