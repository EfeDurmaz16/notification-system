package stream

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/delivery"
	"github.com/redis/go-redis/v9"
)

func TestProviderCooldownRequiresConsecutiveFailuresForOneChannel(t *testing.T) {
	var health providerHealth
	now := time.Now()
	for _, outcome := range []struct {
		status int
		reason string
	}{
		{503, "provider_outcome_unknown"}, {504, "provider_outcome_unknown"}, {202, "provider_accepted"},
		{503, "provider_outcome_unknown"}, {504, "provider_outcome_unknown"}, {422, "provider_rejected"},
		{503, "provider_outcome_unknown"}, {504, "provider_outcome_unknown"},
	} {
		if health.shouldPause("sms", outcome.status, outcome.reason, now) {
			t.Fatalf("healthy response should have reset earlier failures, status=%d", outcome.status)
		}
	}
	if health.shouldPause("email", http.StatusServiceUnavailable, "", now) {
		t.Fatal("email inherited SMS failures")
	}
	if !health.shouldPause("sms", 0, "transport_outcome_unknown", now) {
		t.Fatal("three consecutive SMS endpoint failures should pause new admissions")
	}
	for _, status := range []int{503, 202, 503, 503, 503} {
		if health.shouldPause("sms", status, "", now.Add(providerPause-time.Millisecond)) {
			t.Fatal("result from a pre-pause send extended the cooldown")
		}
	}
	for i := range 3 {
		if got := health.shouldPause("sms", 503, "", now.Add(providerPause)); got != (i == 2) {
			t.Fatalf("post-cooldown failure %d: new pause = %v", i+1, got)
		}
	}
}

func TestProviderFailureStreakUsesOutcomeEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		status       int
		// After two 503s, an endpoint failure trips immediately, a neutral
		// observation needs one more failure and a confirmed result needs three.
		moreFailures int
	}{
		{"server error", "provider_outcome_unknown", 500, 0},
		{"throttled", "provider_outcome_unknown", 429, 0},
		{"unauthorized", "provider_unauthorized", 401, 0},
		{"failed dial", "dial_failed", 0, 0},
		{"lost response", "transport_outcome_unknown", 0, 0},
		{"inaccessible endpoint", "provider_endpoint_access", 404, 0},
		{"malformed receipt", "malformed_ack", 202, 0},
		{"missing receipt", "missing_ack", 200, 0},
		{"unexpected receipt status", "unexpected_ack_status", 200, 0},
		{"request timeout", "provider_outcome_unknown", 408, 1},
		{"conflict", "provider_outcome_unknown", 409, 1},
		{"gone", "provider_outcome_unknown", 410, 1},
		{"redirect", "provider_outcome_unknown", 307, 1},
		{"local deadline", "deadline_before_dispatch", 0, 1},
		{"accepted receipt", "provider_accepted", 202, 3},
		{"content rejection", "provider_rejected", 422, 3},
		{"forbidden recipient", "provider_forbidden", 403, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var h providerHealth
			now := time.Now()
			fail := func() bool { return h.shouldPause("sms", 503, "provider_outcome_unknown", now) }
			for range 2 {
				if fail() {
					t.Fatal("two failures tripped the cooldown")
				}
			}
			paused := h.shouldPause("sms", tc.status, tc.reason, now)
			for i := 0; i < tc.moreFailures; i++ {
				if paused {
					t.Fatalf("tripped after %d additional failures, want %d", i, tc.moreFailures)
				}
				paused = fail()
			}
			if !paused {
				t.Fatalf("cooldown did not trip after %d additional failures", tc.moreFailures)
			}
		})
	}
}

func TestAccessErrorsTripOnlyForEndpointEvidence(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound} {
		var health providerHealth
		now := time.Now()
		for range 4 {
			if health.shouldPause("sms", status, "provider_rejected", now) {
				t.Fatalf("per-recipient failure tripped channel: HTTP %d", status)
			}
		}
		for i := range 3 {
			if got := health.shouldPause("sms", status, "provider_endpoint_access", now); got != (i == 2) {
				t.Fatalf("endpoint failure HTTP %d, attempt %d: pause=%v", status, i+1, got)
			}
		}
	}
}

func TestLocalOutcomesPreserveProviderFailureStreak(t *testing.T) {
	var h providerHealth
	now := time.Now()
	for _, step := range []struct {
		code   int
		reason string
		pause  bool
	}{
		{503, "provider_outcome_unknown", false},
		{0, "deadline_before_dispatch", false},
		{503, "provider_outcome_unknown", false},
		{0, "idempotency_window_expired", false},
		{202, "provider_accepted", false},
		{0, "dial_failed", false},
		{0, "deadline_before_dispatch", false},
		{0, "transport_outcome_unknown", false},
		{0, "deadline_before_dispatch", false},
		{503, "provider_outcome_unknown", true},
	} {
		if got := h.shouldPause("sms", step.code, step.reason, now); got != step.pause {
			t.Fatalf("status=%d reason=%s pause=%t, want %t", step.code, step.reason, got, step.pause)
		}
	}
}

type trackedOutcomes map[string]delivery.Outcome

func (s trackedOutcomes) Start(_ context.Context, id string, _ int) (bool, error) {
	_, exists := s[id]
	return !exists, nil
}

func (s trackedOutcomes) Finish(_ context.Context, id string, _ int, outcome delivery.Outcome) error {
	s[id] = outcome
	return nil
}

func TestRedisProviderCooldownAcrossReplicas(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR to verify worker cooldown across replicas")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("test:outage:%d:", time.Now().UnixNano())
	newReplica := func() limiter {
		client := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1})
		t.Cleanup(func() { _ = client.Close() })
		return limiter{client: client, prefix: prefix}
	}
	first := providerHealth{limit: newReplica()}
	second := newReplica()
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		posts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	outcomes := trackedOutcomes{}
	engine := delivery.Engine{Store: outcomes, URL: server.URL, Client: server.Client(), ObserveEndpoint: first.observe}
	for i := range 3 {
		job := delivery.Job{ID: fmt.Sprintf("outage-%d", i), Channel: "sms", Attempt: 1}
		if err := engine.Send(ctx, job); err != nil {
			t.Fatal(err)
		}
		if outcome := outcomes[job.ID]; outcome.State != "unknown" || !outcome.NextAttemptAt.IsZero() {
			t.Fatalf("expected an unresolved POST without automatic redrive, got %+v", outcome)
		}
	}
	if posts.Load() != 3 {
		t.Fatalf("provider calls=%d, want one for each affected job", posts.Load())
	}
	key := prefix + "{sms}:pause"
	if ttl, err := second.client.PTTL(ctx, key).Result(); err != nil || ttl < 29*time.Second || ttl > providerPause {
		t.Fatalf("pause expiry=%v err=%v", ttl, err)
	}
	if slot, wait, err := second.tryAcquire(ctx, "sms"); err != nil || wait <= 0 || slot.token != "" {
		t.Fatalf("expected paused channel: slot=%+v wait=%v err=%v", slot, wait, err)
	}
	if _, wait, err := second.tryAcquire(ctx, "email"); err != nil || wait != 0 {
		t.Fatalf("unaffected channel failed: %v", err)
	}
	// Advance just this test key to its expiry without waiting 30 seconds.
	if err := first.limit.client.PExpire(ctx, key, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, wait, err := second.tryAcquire(ctx, "sms"); err != nil || wait != 0 {
		t.Fatalf("SMS admission failed after cooldown: %v", err)
	}
}
