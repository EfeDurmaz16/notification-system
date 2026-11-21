package delivery

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type stopAfterClaim struct {
	recordStore
	stop context.CancelFunc
}

func (s *stopAfterClaim) Start(ctx context.Context, id string, attempt int) (bool, error) {
	claimed, err := s.recordStore.Start(ctx, id, attempt)
	s.stop()
	return claimed, err
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestShutdownAfterClaimPreservesProviderDeadlineAndReceipt(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	ctx, stop := context.WithDeadline(t.Context(), deadline)
	defer stop()
	store := &stopAfterClaim{stop: stop}
	var posts int
	client := &http.Client{Timeout: time.Minute, Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		posts++
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		if got, ok := r.Context().Deadline(); !ok || !got.Equal(deadline) {
			t.Errorf("provider deadline = %v, want %v", got, deadline)
		}
		return acceptedTransport{}.RoundTrip(r)
	})}
	engine := Engine{Store: store, Client: client, URL: "http://provider.invalid/"}
	if err := engine.Send(ctx, initialJob()); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(ctx.Err(), context.Canceled) || posts != 1 || store.state != accepted || store.messageID != "m1" {
		t.Fatalf("worker=%v POSTs=%d status=%q receipt=%q", ctx.Err(), posts, store.state, store.messageID)
	}
}

func TestShutdownBeforeClaimLeavesJobPending(t *testing.T) {
	ctx, stop := context.WithCancel(t.Context())
	stop()
	store := new(recordStore)
	engine := Engine{Store: store, URL: "http://provider.invalid/", Client: &http.Client{Transport: acceptedTransport{}}}
	if err := engine.Send(ctx, initialJob()); !errors.Is(err, context.Canceled) {
		t.Fatalf("send error = %v, want cancellation", err)
	}
	if store.starts != 0 || store.finishes != 0 {
		t.Fatalf("new work after shutdown: %+v", store)
	}
}

// gateStore models the database's conditional pending -> sending update. Pausing
// before that update lets the test schedule a successful cancellation first.
type gateStore struct {
	beforeClaim   chan struct{}
	continueClaim chan struct{}
	state         atomic.Value
}

func (s *gateStore) Start(context.Context, string, int) (bool, error) {
	close(s.beforeClaim)
	<-s.continueClaim
	return s.state.CompareAndSwap("pending", "sending"), nil
}

func (s *gateStore) Finish(_ context.Context, _ string, _ int, out Outcome) error {
	s.state.Store(out.State)
	return nil
}

func TestCancellationWinsZeroProviderPOSTs(t *testing.T) {
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m1","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`))
	}))
	defer server.Close()

	store := &gateStore{beforeClaim: make(chan struct{}), continueClaim: make(chan struct{})}
	store.state.Store("pending")
	engine := Engine{Store: store, URL: server.URL, Client: server.Client()}
	done := make(chan error, 1)
	go func() {
		done <- engine.Send(t.Context(), Job{ID: "n1", Channel: "sms", Recipient: "+15555555555", Content: "hello", Attempt: 1})
	}()
	<-store.beforeClaim
	if !store.state.CompareAndSwap("pending", "cancelled") {
		t.Fatal("cancellation must win the pending transition")
	}
	close(store.continueClaim)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := posts.Load(); got != 0 {
		t.Fatalf("provider POST count = %d, want 0 after cancellation", got)
	}
	if got := store.state.Load(); got != "cancelled" {
		t.Fatalf("final state = %v, want cancelled", got)
	}
}
