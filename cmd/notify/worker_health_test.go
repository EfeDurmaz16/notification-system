package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"
)

func TestWorkerReadiness(t *testing.T) {
	names := []string{"postgres", "redis", "kafka"}
	for _, tc := range []struct {
		name     string
		fail     string
		draining bool
		wantCode int
		wantBody string
		calls    int
	}{
		{"ready", "", false, 200, "", 3},
		{"postgres outage", "postgres", false, 503, "postgres unavailable\n", 1},
		{"redis outage", "redis", false, 503, "redis unavailable\n", 2},
		{"kafka outage", "kafka", false, 503, "kafka unavailable\n", 3},
		{"SIGTERM", "", true, 503, "worker draining\n", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.draining {
				cancel()
			}
			var probes []workerProbe
			var calls []string
			for _, name := range names {
				probes = append(probes, workerProbe{name, func(ctx context.Context) error {
					if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 2*time.Second {
						t.Fatal("expected a bounded readiness probe")
					}
					calls = append(calls, name)
					if name == tc.fail {
						return errors.New("private endpoint detail")
					}
					return nil
				}})
			}
			h := workerHealth(ctx, probes...)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if w.Code != tc.wantCode || w.Body.String() != tc.wantBody || !slices.Equal(calls, names[:tc.calls]) {
				t.Fatalf("got %d %q, probes %v", w.Code, w.Body, calls)
			}
			w = httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/livez", nil))
			if w.Code != http.StatusOK || len(calls) != tc.calls {
				t.Fatal("liveness must not contact dependencies")
			}
		})
	}
}
