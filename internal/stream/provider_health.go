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
	mu       sync.Mutex
	failures map[string]int
	limit    limiter
}

func (h *providerHealth) observe(ctx context.Context, channel string, status int, reason string) error {
	if !h.shouldPause(channel, status, reason) {
		return nil
	}
	key := h.limit.prefix + "{" + channel + "}:pause"
	created, err := h.limit.client.SetNX(ctx, key, "1", providerPause).Result()
	if created {
		slog.WarnContext(ctx, "provider admissions paused", "channel", channel, "cooldown", providerPause, "reason", reason, "provider_http_status", status)
	}
	return err
}

func (h *providerHealth) shouldPause(channel string, status int, reason string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	// 4xx recipient/content rejections still prove the endpoint is answering.
	distressed := status >= 500 || status == http.StatusTooManyRequests ||
		status == http.StatusUnauthorized ||
		reason == "dial_failed" || reason == "transport_outcome_unknown" || reason == "malformed_ack"
	if !distressed {
		delete(h.failures, channel)
		return false
	}
	if h.failures == nil {
		h.failures = make(map[string]int)
	}
	h.failures[channel] = min(3, h.failures[channel]+1)
	return h.failures[channel] == 3
}
