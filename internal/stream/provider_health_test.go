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
	for _, status := range []int{503, 504, 202, 503, 504, 422, 503, 504} {
		if health.shouldPause("sms", status, "") {
			t.Fatalf("healthy response should have reset earlier failures, status=%d", status)
		}
	}
	if health.shouldPause("email", http.StatusServiceUnavailable, "") {
		t.Fatal("email inherited SMS failures")
	}
	if !health.shouldPause("sms", 0, "transport_outcome_unknown") {
		t.Fatal("three consecutive SMS endpoint failures should pause new admissions")
	}
	if health.shouldPause("sms", http.StatusAccepted, "provider_accepted") {
		t.Fatal("acceptance should reset the streak")
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
