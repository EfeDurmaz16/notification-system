package delivery

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestStalledConnectionAcquisitionCanRetryWithoutDuplicatePOST(t *testing.T) {
	var posts, dials atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		posts.Add(1)
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"messageId":"once","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`)
	}))
	defer server.Close()

	// Let the first client deadline expire with no connection. The second
	// attempt uses an actual socket so the fixture independently counts POSTs.
	release := make(chan struct{})
	transport := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		if dials.Add(1) == 1 {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil, errors.New("first dial never acquired a connection")
		}
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}
	defer transport.CloseIdleConnections()
	store := new(recordStore)
	engine := Engine{Store: store, Client: &http.Client{Transport: transport, Timeout: 150 * time.Millisecond}, URL: server.URL}
	job := initialJob()
	err := engine.Send(t.Context(), job)
	close(release)
	if err != nil || store.state != retryWait || posts.Load() != 0 || dials.Load() != 1 {
		t.Fatalf("stalled dial: err=%v state=%s POSTs=%d dials=%d", err, store.state, posts.Load(), dials.Load())
	}
	job.Attempt++
	engine.Client.Timeout = 3 * time.Second
	if err := engine.Send(t.Context(), job); err != nil || store.state != accepted || posts.Load() != 1 {
		t.Fatalf("recovery: err=%v state=%s POSTs=%d", err, store.state, posts.Load())
	}
}

func TestProviderRetryAfterRequiresSafeRetry(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		code                        int
		hint                        string
		throttleRejects, idempotent bool
		keyWindow                   time.Duration
		want                        string
	}{
		{"unverified 429", 429, "30", false, false, 0, unknown},
		{"contractual 429", 429, "30", true, false, 0, retryWait},
		{"idempotent 503", 503, "30", false, true, time.Hour, retryWait},
		{"ambiguous 503", 503, "30", true, false, 0, unknown},
		{"ambiguous gateway timeout", 504, "", true, false, 0, unknown},
		{"idempotency expires first", 503, "30", false, true, 15 * time.Second, unknown},
		{"provider delay exceeds retention", 429, "86401", true, false, 0, failed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", tc.hint)
				w.WriteHeader(tc.code)
			}))
			defer server.Close()
			store := new(recordStore)
			engine := Engine{Store: store, Client: server.Client(), URL: server.URL, ThrottleRejects: tc.throttleRejects, Idempotent: tc.idempotent, IdempotencyWindow: tc.keyWindow}
			start := time.Now()
			if err := engine.Send(t.Context(), initialJob()); err != nil {
				t.Fatal(err)
			}
			if store.state != tc.want {
				t.Fatalf("got %s, want %s", store.state, tc.want)
			}
			if tc.want == retryWait && store.due.Before(start.Add(30*time.Second)) {
				t.Fatalf("retry at %s ignores provider minimum", store.due)
			}
			if tc.want != retryWait && !store.due.IsZero() {
				t.Fatalf("unexpected retry at %s", store.due)
			}
		})
	}
}

func TestRetryAfterHeaderFormats(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		input string
		want  time.Duration
	}{
		{"15", 15 * time.Second}, {now.Add(time.Minute).Format(http.TimeFormat), time.Minute},
		{now.Add(-time.Minute).Format(http.TimeFormat), 0}, {"n/a", 0}, {"-5", 0},
		{"184467440737095516150", maxRetryAfter + time.Second},
	} {
		if got := retryAfter(tc.input, now); got != tc.want {
			t.Errorf("retryAfter(%q) = %s, want %s", tc.input, got, tc.want)
		}
	}
}

func TestDefaultBackoffCanSpanThirtySecondOutage(t *testing.T) {
	e := Engine{}
	var total time.Duration
	for attempt := 1; attempt < e.maxAttempts(); attempt++ {
		delay := e.backoff(attempt)
		ceiling := DefaultBaseBackoff << (attempt - 1)
		if delay < ceiling/2 || delay > ceiling {
			t.Fatalf("attempt %d delay %s outside [%s, %s]", attempt, delay, ceiling/2, ceiling)
		}
		total += delay
	}
	if total <= 30*time.Second {
		t.Fatalf("total retry wait %s", total)
	}
}

func TestClientTimeoutAfterProviderReceivedPOSTRemainsUnknown(t *testing.T) {
	var posts atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		posts.Add(1)
		<-release
	}))
	defer server.Close()
	defer close(release)
	client := server.Client()
	client.Timeout = 500 * time.Millisecond
	store := new(recordStore)
	engine := Engine{Store: store, Client: client, URL: server.URL}
	if err := engine.Send(t.Context(), initialJob()); err != nil {
		t.Fatal(err)
	}
	if posts.Load() != 1 || store.state != unknown || !store.due.IsZero() {
		t.Fatalf("lost response after POST: state=%s POSTs=%d due=%v", store.state, posts.Load(), store.due)
	}
}

func TestDeadlineBeforeDispatchDoesNotContactProvider(t *testing.T) {
	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	req, err := makeRequest(expired, "https://provider.invalid/", initialJob(), false)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("expired request reached transport")
		return nil, nil
	})}
	out := dispatch(client, req)
	if !out.safeRetry || out.reason != "deadline_before_dispatch" {
		t.Fatalf("unexpected outcome: %+v", out)
	}
}
