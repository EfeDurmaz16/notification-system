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
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/observability"
)

const (
	accepted  = "accepted"
	unknown   = "unknown"
	failed    = "failed"
	retryWait = "retry_wait"
)

// Job is one dispatch attempt. ID and the payload are immutable across retries.
// Attempt starts at 1 and matches the generation on the original outbox event.
type Job struct {
	ID, Recipient, Channel, Content, Priority string
	Traceparent                               string
	Attempt                                   int
	FirstAttemptAt                            time.Time
}

// Start compares the pending state and expected attempt before setting sending.
// Finish must update only the matching sending generation.
type Store interface {
	Start(context.Context, string, int) (bool, error)
	Finish(context.Context, string, int, string, string, time.Time) error
}

type Engine struct {
	Store  Store
	Client *http.Client
	URL    string
	// Idempotent requires an explicit guarantee from the external provider.
	Idempotent        bool
	IdempotencyWindow time.Duration
	MaxAttempts       int
	BaseBackoff       time.Duration
}

// Send is called after the worker has obtained a shared channel permit.
func (e *Engine) Send(ctx context.Context, j Job) error {
	ctx, end := observability.Delivery(ctx, j.Traceparent, j.ID)
	defer end()
	request, err := makeRequest(ctx, e.URL, j, e.Idempotent)
	if err != nil {
		return fmt.Errorf("construct provider request: %w", err)
	}
	claimed, err := e.Store.Start(ctx, j.ID, j.Attempt)
	if err != nil {
		return fmt.Errorf("claim send: %w", err)
	}
	if !claimed {
		return nil
	}

	client := http.Client{}
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
		out = dispatch(&client, request)
	}
	next := time.Time{}
	if out.safeRetry || (out.state == unknown && e.Idempotent) {
		delay := e.backoff(j.Attempt)
		candidate := time.Now().Add(delay)
		if j.Attempt < e.maxAttempts() && (!e.Idempotent || candidate.Add(client.Timeout).Before(until)) {
			out.state = retryWait
			next = candidate
		} else if out.safeRetry && !(e.Idempotent && j.Attempt > 1) {
			out.state = failed
		}
	}
	// A timed-out request can already have taken effect. Record the outcome even
	// when a worker is shutting down; never use the canceled provider context.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := e.Store.Finish(finishCtx, j.ID, j.Attempt, out.state, out.messageID, next); err != nil {
		return fmt.Errorf("persist send result: %w", err)
	}
	level := slog.LevelDebug
	if out.state != accepted {
		level = slog.LevelWarn
	}
	slog.Log(ctx, level, "provider result", "notification_id", j.ID, "channel", j.Channel,
		"attempt", j.Attempt, "status", out.state, "reason", out.reason, "provider_http_status", out.httpStatus)
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
	return 4
}

func (e *Engine) backoff(attempt int) time.Duration {
	base := e.BaseBackoff
	if base <= 0 {
		base = time.Second
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
}

func dispatch(client *http.Client, req *http.Request) result {
	req, end := observability.ProviderRequest(req)
	defer end()
	resp, err := client.Do(req)
	if err != nil {
		var op *net.OpError
		if errors.As(err, &op) && op.Op == "dial" {
			return result{state: unknown, safeRetry: true, reason: "dial_failed"}
		}
		return result{state: unknown, reason: "transport_outcome_unknown"}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		switch resp.StatusCode {
		case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
			http.StatusMethodNotAllowed, http.StatusRequestEntityTooLarge, http.StatusUnsupportedMediaType, http.StatusUnprocessableEntity:
			return result{state: failed, reason: "provider_rejected", httpStatus: resp.StatusCode}
		default:
			return result{state: unknown, reason: "provider_outcome_unknown", httpStatus: resp.StatusCode}
		}
	}
	const maxBody = 16 << 10
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	var ack struct {
		MessageID string    `json:"messageId"`
		Status    string    `json:"status"`
		Timestamp time.Time `json:"timestamp"`
	}
	if err != nil || len(body) > maxBody || json.Unmarshal(body, &ack) != nil || ack.MessageID == "" || ack.Status != "accepted" || ack.Timestamp.IsZero() {
		return result{state: unknown, reason: "malformed_ack", httpStatus: resp.StatusCode}
	}
	return result{state: accepted, messageID: ack.MessageID, reason: "provider_accepted", httpStatus: resp.StatusCode}
}
