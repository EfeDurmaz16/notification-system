package delivery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

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

func (s *gateStore) Finish(_ context.Context, _ string, _ int, state, _ string, _ time.Time) error {
	s.state.Store(state)
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
