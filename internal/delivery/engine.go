// Package delivery owns the last durable state transition before an external POST.
package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/observability"
)

const (
	DefaultMaxAttempts = 6
	DefaultBaseBackoff = 2 * time.Second
	maxRetryAfter      = 24 * time.Hour

	accepted  = "accepted"
	unknown   = "unknown"
	failed    = "failed"
	retryWait = "retry_wait"
)

// Job is one dispatch attempt. ID and the payload are immutable across retries.
// Attempt starts at 1 and matches the generation on the original outbox event.
type Job struct {
	ID, Recipient, Channel, Content, Priority, Status string
	Traceparent, CorrelationID                        string
	Attempt                                           int
	FirstAttemptAt                                    time.Time
}

// Outcome records the most recent resolved or ambiguous dispatch attempt. HTTPStatus
// is zero if there was no HTTP response. Reason is a code chosen by the engine.
type Outcome struct {
	State, MessageID, Reason string
	HTTPStatus               int
	NextAttemptAt            time.Time
}

// Start compares the pending state and expected attempt before setting sending.
// Finish must update only the matching sending generation.
type Store interface {
	Start(context.Context, string, int) (bool, error)
	Finish(context.Context, string, int, Outcome) error
}

type Engine struct {
	// ObserveEndpoint lets workers pause fresh admissions after provider outages.
	ObserveEndpoint func(context.Context, string, int, string) error
	Store           Store
	Client          *http.Client
	URL             string
	// Idempotent requires an explicit guarantee from the external provider.
	Idempotent bool
	// ThrottleRejects requires a provider contract guaranteeing a 429 has no side effect.
	ThrottleRejects bool
	// MessageIDReceipt is for destinations that define a JSON messageId in a
	// successful response as proof of acceptance. The assessment uses a full 202 receipt.
	MessageIDReceipt  bool
	IdempotencyWindow time.Duration
	MaxAttempts       int
	BaseBackoff       time.Duration
}

// Send is called after the worker has obtained a shared channel permit.
// Shutdown stops new claim attempts. Once a claim is in flight, await its
// database reply and dispatch only if the matching pending row was claimed.
func (e *Engine) Send(ctx context.Context, j Job) error {
	ctx, end := observability.Delivery(ctx, j.Traceparent, j.ID)
	defer end()
	providerCtx := context.WithoutCancel(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		providerCtx, cancel = context.WithDeadline(providerCtx, deadline)
		defer cancel()
	}
	request, err := makeRequest(providerCtx, e.URL, j, e.Idempotent)
	if err != nil {
		return fmt.Errorf("construct provider request: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// PostgreSQL may have committed before its reply reaches us. Preserve the
	// original send deadline while letting an in-flight claim drain on SIGTERM.
	claimCtx, cancelClaim := context.WithTimeout(providerCtx, 30*time.Second)
	claimed, err := e.Store.Start(claimCtx, j.ID, j.Attempt)
	cancelClaim()
	if err != nil {
		return fmt.Errorf("claim send: %w", err)
	}
	if !claimed {
		return nil
	}

	client := http.Client{Transport: providerTransport}
	if e.Client != nil {
		client = *e.Client
	}
	if client.Timeout <= 0 {
		client.Timeout = 5 * time.Second
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	now := time.Now()
	until := e.keyDeadline(j, now)
	out := result{state: unknown, reason: "idempotency_window_expired"}
	if !e.Idempotent || now.Add(client.Timeout).Before(until) {
		out = e.dispatch(&client, request)
		if e.ObserveEndpoint != nil {
			observeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			err := e.ObserveEndpoint(observeCtx, j.Channel, out.httpStatus, out.reason)
			cancel()
			if err != nil {
				slog.WarnContext(ctx, "could not pause provider admissions", "channel", j.Channel, "error", err)
			}
		}
		if e.ThrottleRejects && out.httpStatus == http.StatusTooManyRequests {
			out.safeRetry = true
			out.reason = "provider_throttled"
		}
	}
	next := time.Time{}
	if out.safeRetry || (out.state == unknown && e.Idempotent) {
		delay := max(e.backoff(j.Attempt), out.retryAfter)
		candidate := time.Now().Add(delay)
		if delay <= maxRetryAfter && j.Attempt < e.maxAttempts() && (!e.Idempotent || candidate.Add(client.Timeout).Before(until)) {
			out.state = retryWait
			next = candidate
		} else if out.safeRetry {
			out.state = failed
		}
	}
	// A later rejection (for example after credentials expire) only describes
	// the current attempt. Without a persisted definitive outcome for every
	// earlier attempt, stop retrying and retain the original uncertainty.
	if e.Idempotent && j.Attempt > 1 && out.state == failed {
		out.state = unknown
		out.reason = "prior_attempt_unresolved"
	}
	// A timed-out request can already have taken effect. Record the outcome even
	// when a worker is shutting down; never use the canceled provider context.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	outcome := Outcome{State: out.state, MessageID: out.messageID, Reason: out.reason, HTTPStatus: out.httpStatus, NextAttemptAt: next}
	finishErr := e.Store.Finish(finishCtx, j.ID, j.Attempt, outcome)
	level := slog.LevelInfo
	if finishErr != nil {
		level = slog.LevelError
	} else if out.state != accepted {
		level = slog.LevelWarn
	}
	slog.Log(ctx, level, "provider result", "correlation_id", j.CorrelationID, "trace_id", observability.TraceID(ctx),
		"notification_id", j.ID, "channel", j.Channel, "attempt", j.Attempt, "status", out.state,
		"reason", out.reason, "provider_http_status", out.httpStatus, "provider_message_id", out.messageID,
		"persistence_confirmed", finishErr == nil)
	if finishErr != nil {
		return fmt.Errorf("persist send result: %w", finishErr)
	}
	return nil
}

func (e *Engine) keyDeadline(j Job, now time.Time) time.Time {
	start := j.FirstAttemptAt
	if start.IsZero() {
		if j.Attempt > 1 {
			return time.Time{}
		}
		start = now
	}
	window := e.IdempotencyWindow
	if window <= 0 {
		window = 24 * time.Hour
	}
	return start.Add(window)
}

func (e *Engine) maxAttempts() int {
	if e.MaxAttempts > 0 {
		return e.MaxAttempts
	}
	return DefaultMaxAttempts
}

func (e *Engine) backoff(attempt int) time.Duration {
	base := e.BaseBackoff
	if base <= 0 {
		base = DefaultBaseBackoff
	}
	base = min(base, time.Minute)
	for i := 1; i < attempt && base < time.Minute; i++ {
		base = min(2*base, time.Minute)
	}
	half := base / 2
	return half + time.Duration(rand.Int64N(int64(base-half)+1))
}

func makeRequest(ctx context.Context, endpoint string, j Job, idempotent bool) (*http.Request, error) {
	payload, err := json.Marshal(struct {
		To      string `json:"to"`
		Channel string `json:"channel"`
		Content string `json:"content"`
	}{j.Recipient, j.Channel, j.Content})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	// Keep net/http from replaying a consumed body outside the shared rate
	// limiter. In particular, Idempotency-Key makes Go treat a POST as replayable
	// when it can recreate the body.
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Notification-ID", j.ID)
	if idempotent {
		req.Header.Set("Idempotency-Key", j.ID)
	}
	return req, nil
}

type result struct {
	state, messageID, reason string
	httpStatus               int
	safeRetry                bool
	retryAfter               time.Duration
}

func (e *Engine) dispatch(client *http.Client, req *http.Request) result {
	req, end := observability.ProviderRequest(req)
	defer end()
	if req.Context().Err() != nil {
		return result{state: unknown, safeRetry: true, reason: "deadline_before_dispatch"}
	}
	// The standard transport calls GetConn before acquisition and GotConn
	// before using the connection. A client deadline can mask a stalled dial.
	// Only classify it as unsent when acquisition started and never succeeded.
	var acquiring, acquired atomic.Bool
	trace := &httptrace.ClientTrace{
		GetConn: func(string) { acquiring.Store(true) },
		GotConn: func(httptrace.GotConnInfo) { acquired.Store(true) },
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := client.Do(req)
	if err != nil {
		var op *net.OpError
		if !acquired.Load() && (acquiring.Load() || (errors.As(err, &op) && op.Op == "dial")) {
			return result{state: unknown, safeRetry: true, reason: "dial_failed"}
		}
		return result{state: unknown, reason: "transport_outcome_unknown"}
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return readReceipt(resp, e.MessageIDReceipt)
	}
	// Webhook.site permits a capture URL to return a configured error status.
	// The same status can also signal an expired token or denied endpoint access.
	if strings.EqualFold(req.URL.Hostname(), "webhook.site") && (resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound) {
		return result{state: unknown, reason: "provider_endpoint_access", httpStatus: resp.StatusCode}
	}
	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return result{state: failed, reason: "provider_unauthorized", httpStatus: resp.StatusCode}
	case http.StatusForbidden:
		return result{state: failed, reason: "provider_forbidden", httpStatus: resp.StatusCode}
	case http.StatusBadRequest, http.StatusNotFound,
		http.StatusMethodNotAllowed, http.StatusRequestEntityTooLarge, http.StatusUnsupportedMediaType, http.StatusUnprocessableEntity:
		return result{state: failed, reason: "provider_rejected", httpStatus: resp.StatusCode}
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return result{state: unknown, reason: "provider_outcome_unknown", httpStatus: resp.StatusCode,
			retryAfter: retryAfter(resp.Header.Get("Retry-After"), time.Now())}
	default:
		return result{state: unknown, reason: "provider_outcome_unknown", httpStatus: resp.StatusCode}
	}
}

func readReceipt(resp *http.Response, messageIDReceipt bool) result {
	const maxBody = 16 << 10
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	invalid := result{state: unknown, reason: "malformed_ack", httpStatus: resp.StatusCode}
	if err != nil || len(body) > maxBody {
		return invalid
	}
	if len(bytes.TrimSpace(body)) == 0 {
		invalid.reason = "missing_ack"
		return invalid
	}
	if !messageIDReceipt && resp.StatusCode != http.StatusAccepted {
		invalid.reason = "unexpected_ack_status"
		return invalid
	}
	var ack struct {
		MessageID string          `json:"messageId"`
		Status    json.RawMessage `json:"status"`
		Timestamp json.RawMessage `json:"timestamp"`
	}
	if json.Unmarshal(body, &ack) != nil || strings.TrimSpace(ack.MessageID) == "" {
		return invalid
	}
	if !messageIDReceipt || ack.Status != nil {
		var status string
		if json.Unmarshal(ack.Status, &status) != nil || status != accepted {
			return invalid
		}
	}
	if !messageIDReceipt {
		var timestamp time.Time
		if json.Unmarshal(ack.Timestamp, &timestamp) != nil || timestamp.IsZero() {
			return invalid
		}
	}
	return result{state: accepted, messageID: ack.MessageID, reason: "provider_accepted", httpStatus: resp.StatusCode}
}

// retryAfter interprets both RFC 9110 forms. A valid hint above our retention
// budget must stop automatic retries, never shorten the provider's delay.
func retryAfter(value string, now time.Time) time.Duration {
	if value == "" {
		return 0
	}
	seconds, err := strconv.ParseUint(value, 10, 64)
	if err == nil || errors.Is(err, strconv.ErrRange) {
		if seconds > uint64(maxRetryAfter/time.Second) {
			return maxRetryAfter + time.Second
		}
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0
	}
	return max(0, when.Sub(now))
}

// All workers in a process send to one configured endpoint. Reuse its idle
// connections and reserve time for the request after establishing TCP.
var providerTransport = func() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	transport.MaxIdleConnsPerHost = transport.MaxIdleConns
	return transport
}()
