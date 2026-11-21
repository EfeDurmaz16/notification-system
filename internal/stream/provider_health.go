package stream

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

const providerPause = 30 * time.Second

// Run owns one monitor per worker replica. A healthy channel makes no extra
// Redis calls. Once a replica sees consecutive endpoint failures, all replicas
// observe the same cooldown when asking Redis for a channel permit.
type providerHealth struct {
	mu      sync.Mutex
	streaks map[string]failureStreak
	limit   limiter
}

type failureStreak struct {
	count       int
	pausedUntil time.Time
}

func (h *providerHealth) observe(ctx context.Context, channel string, status int, reason string) error {
	now := time.Now()
	if !h.shouldPause(channel, status, reason, now) {
		return nil
	}
	key := h.limit.prefix + "{" + channel + "}:pause"
	created, err := h.limit.client.SetNX(ctx, key, "1", providerPause).Result()
	if err != nil {
		h.mu.Lock()
		// Restore eligibility after a failed write, but only for our trip.
		if streak := h.streaks[channel]; streak.pausedUntil.Equal(now.Add(providerPause)) {
			h.streaks[channel] = failureStreak{count: 2}
		}
		h.mu.Unlock()
	}
	if created {
		slog.WarnContext(ctx, "provider admissions paused", "channel", channel, "cooldown", providerPause, "reason", reason, "provider_http_status", status)
	}
	return err
}

func (h *providerHealth) shouldPause(channel string, status int, reason string, now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	streak := h.streaks[channel]
	// Results from concurrent sends admitted before the pause must not prime
	// the next streak. Require three fresh failures when admission resumes.
	if now.Before(streak.pausedUntil) {
		return false
	}
	distressed := status >= 500 || status == http.StatusTooManyRequests ||
		status == http.StatusUnauthorized ||
		reason == "dial_failed" || reason == "transport_outcome_unknown" || reason == "provider_endpoint_access" ||
		reason == "malformed_ack" || reason == "missing_ack" || reason == "unexpected_ack_status"
	if !distressed {
		// Only an accepted receipt or a definitive recipient/content rejection
		// clears the streak. Local outcomes and ambiguous HTTP replies are neutral.
		switch reason {
		case "provider_accepted", "provider_rejected", "provider_forbidden":
			delete(h.streaks, channel)
		}
		return false
	}
	if h.streaks == nil {
		h.streaks = make(map[string]failureStreak)
	}
	streak.count++
	if streak.count == 3 {
		h.streaks[channel] = failureStreak{pausedUntil: now.Add(providerPause)}
		return true
	}
	h.streaks[channel] = streak
	return false
}
