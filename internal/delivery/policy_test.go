package delivery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type recordStore struct {
	starts, finishes         int
	state, messageID, reason string
	httpStatus               int
	due                      time.Time
	startErr, finishErr      error
}

func (s *recordStore) Start(context.Context, string, int) (bool, error) {
	s.starts++
	return true, s.startErr
}
func (s *recordStore) Finish(_ context.Context, _ string, _ int, out Outcome) error {
	s.finishes++
	s.state, s.messageID, s.due = out.State, out.MessageID, out.NextAttemptAt
	s.reason, s.httpStatus = out.Reason, out.HTTPStatus
	return s.finishErr
}

func initialJob() Job {
	return Job{ID: "stable-id", Recipient: "user@example.com", Content: "hello", Channel: "email", Priority: "high", Attempt: 1}
}

func TestProviderContract(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		idempotent bool
		state      string
	}{
		{"accepted", 202, `{"messageId":"msg123","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`, false, accepted},
		{"unexpected HTTP status with a receipt", 200, `{"messageId":"msg123","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`, false, unknown},
		{"format placeholder instead of a date", 202, `{"messageId":"msg123","status":"accepted","timestamp":"ISO8601"}`, false, unknown},
		{"incomplete ack", 202, `{"messageId":"msg123","status":"accepted"}`, false, unknown},
		{"malformed ack", 202, "{", false, unknown},
		{"rejected", 400, "{}", false, failed},
		{"relay error", 502, "{}", false, unknown},
		{"idempotent relay error", 502, "{}", true, retryWait},
		{"idempotent throttled", 429, "{}", true, retryWait},
		{"webhook throttled", 429, "{}", false, unknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := initialJob()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Notification-ID") != job.ID || r.Method != "POST" {
					t.Errorf("wrong provider identity or method")
				}
				if got := r.Header.Get("Idempotency-Key"); (tc.idempotent && got != job.ID) || (!tc.idempotent && got != "") {
					t.Errorf("unexpected provider key %q", got)
				}
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			store := new(recordStore)
			engine := Engine{Store: store, URL: server.URL, Client: server.Client(), Idempotent: tc.idempotent}
			if err := engine.Send(t.Context(), job); err != nil {
				t.Fatal(err)
			}
			if store.starts != 1 || store.finishes != 1 || store.state != tc.state {
				t.Fatalf("store=%+v want %s", store, tc.state)
			}
			if store.reason == "" || store.httpStatus != tc.status {
				t.Fatalf("missing attempt evidence: %+v", store)
			}
			if (tc.state == retryWait) != store.due.After(time.Now()) {
				t.Fatalf("unexpected retry timestamp %v", store.due)
			}
			if tc.state == accepted && store.messageID != "msg123" {
				t.Fatalf("wrong provider ID %s", store.messageID)
			}
		})
	}
}

func TestLostAckRetriesOnlyWithProviderGuarantee(t *testing.T) {
	for _, idempotent := range []bool{false, true} {
		t.Run(fmt.Sprintf("idempotent_%t", idempotent), func(t *testing.T) {
			first := initialJob()
			var posts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if posts.Add(1) == 1 {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
					return
				}
				if r.Header.Get("Idempotency-Key") != first.ID {
					t.Error("retry changed provider key")
				}
				w.WriteHeader(202)
				_, _ = fmt.Fprint(w, `{"messageId":"original","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`)
			}))
			defer server.Close()
			store := new(recordStore)
			engine := Engine{URL: server.URL, Client: server.Client(), Store: store, Idempotent: idempotent}
			if err := engine.Send(t.Context(), first); err != nil {
				t.Fatal(err)
			}
			if posts.Load() != 1 {
				t.Fatalf("unpermitted transport resend count %d", posts.Load())
			}
			if !idempotent {
				if store.state != unknown {
					t.Fatalf("got %s", store.state)
				}
				return
			}
			if store.state != retryWait {
				t.Fatalf("got %s", store.state)
			}
			first.Attempt++
			first.FirstAttemptAt = time.Now().Add(-time.Second)
			if err := engine.Send(t.Context(), first); err != nil {
				t.Fatal(err)
			}
			if posts.Load() != 2 || store.state != accepted || store.messageID != "original" {
				t.Fatalf("posts=%d state=%s id=%s", posts.Load(), store.state, store.messageID)
			}
		})
	}
}

func TestNeverFollowProviderRedirect(t *testing.T) {
	var destinationPosts atomic.Int32
	dst := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { destinationPosts.Add(1) }))
	defer dst.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, dst.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	store := new(recordStore)
	engine := Engine{URL: server.URL, Client: server.Client(), Store: store}
	if err := engine.Send(t.Context(), initialJob()); err != nil {
		t.Fatal(err)
	}
	if store.state != unknown || destinationPosts.Load() != 0 {
		t.Fatalf("state=%s destination=%d", store.state, destinationPosts.Load())
	}
}

func TestPreDispatchFailureAndRetryExhaustion(t *testing.T) {
	client := &http.Client{Transport: &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Err: errors.New("unavailable")}
	}}}
	e := Engine{URL: "http://provider.invalid/", Client: client, MaxAttempts: 2}
	j := initialJob()
	for _, state := range []string{retryWait, failed} {
		s := new(recordStore)
		e.Store = s
		if err := e.Send(t.Context(), j); err != nil {
			t.Fatal(err)
		}
		if s.state != state {
			t.Fatalf("attempt=%d state=%s want=%s", j.Attempt, s.state, state)
		}
		j.Attempt++
	}
}

func TestExpiredOrMissingIdempotencyWindowPreventsPOST(t *testing.T) {
	now := time.Now()
	for _, started := range []time.Time{now.Add(-25 * time.Hour), {}} {
		var posts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { posts.Add(1) }))
		s := new(recordStore)
		e := Engine{URL: server.URL, Client: server.Client(), Store: s, Idempotent: true}
		j := initialJob()
		j.Attempt = 2
		j.FirstAttemptAt = started
		if err := e.Send(t.Context(), j); err != nil {
			t.Fatal(err)
		}
		server.Close()
		if posts.Load() != 0 || s.state != unknown || !s.due.IsZero() {
			t.Fatalf("posts=%d store=%+v", posts.Load(), s)
		}
	}
}

func TestStoreErrorsPropagate(t *testing.T) {
	errDB := errors.New("database failure")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(202) }))
	defer server.Close()
	for _, s := range []*recordStore{{startErr: errDB}, {finishErr: errDB}} {
		e := Engine{URL: server.URL, Client: server.Client(), Store: s}
		if err := e.Send(t.Context(), initialJob()); !errors.Is(err, errDB) {
			t.Fatal(err)
		}
	}
}

func BenchmarkBuildProviderRequest(b *testing.B) {
	j := initialJob()
	b.ReportAllocs()
	for b.Loop() {
		req, err := makeRequest(b.Context(), "http://localhost/provider", j, true)
		if err != nil {
			b.Fatal(err)
		}
		_ = req.Body.Close()
	}
}

// A later rejection can be produced before the provider checks its idempotency
// cache, for example after a credential expires between attempts.
func TestRetryRejectionPreservesUnknown(t *testing.T) {
	job := initialJob()
	var posts atomic.Int32
	var acceptedByProvider atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Idempotency-Key") != job.ID {
			t.Error("provider received unexpected key")
		}
		if posts.Add(1) == 1 {
			acceptedByProvider.Store(true)
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	store := new(recordStore)
	engine := Engine{URL: server.URL, Client: server.Client(), Store: store, Idempotent: true}
	job.FirstAttemptAt = time.Now()
	if err := engine.Send(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	if store.state != retryWait {
		t.Fatalf("lost ack: state=%s, want retry_wait", store.state)
	}
	job.Attempt++
	if err := engine.Send(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	if !acceptedByProvider.Load() || posts.Load() != 2 || store.state != unknown || !store.due.IsZero() {
		t.Fatalf("after retry rejected: provider accepted=%t posts=%d state=%s next=%v, want accepted=true posts=2 state=unknown and no retry", acceptedByProvider.Load(), posts.Load(), store.state, store.due)
	}
}
