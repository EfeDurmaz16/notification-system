package main

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/logging"
)

func TestSchedulerOutageLogsFirstRepeatAndRecovery(t *testing.T) {
	var out bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&out, nil))
	var outage schedulerOutage
	start := time.Now()
	ctx := t.Context()
	outage.observe(ctx, start, nil, log)
	for _, delay := range []time.Duration{0, time.Millisecond, 29 * time.Second, 30 * time.Second} {
		outage.observe(ctx, start.Add(delay), driver.ErrBadConn, log)
	}
	outage.observe(ctx, start.Add(time.Minute), nil, log)
	outage.observe(ctx, start.Add(time.Minute+time.Second), nil, log)
	outage.observe(ctx, start.Add(time.Minute+2*time.Second), errors.New("private DSN"), log)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	outage.observe(cancelled, start.Add(2*time.Minute), context.Canceled, log)
	if got := out.String(); strings.Count(got, "schedule due work") != 3 ||
		strings.Count(got, "scheduler recovered") != 1 || !strings.Contains(got, `"outage_ms":60000`) ||
		!strings.Contains(got, `"failed_ticks":4`) || !strings.Contains(got, "connection_lost") ||
		strings.Contains(got, "private DSN") {
		t.Fatalf("unexpected outage events: %s", got)
	}
}

func TestRedisDiagnosticsSanitizeAndThrottle(t *testing.T) {
	var out bytes.Buffer
	log := &redisDiagnostics{logger: slog.New(slog.NewJSONHandler(&out, nil))}
	redis.SetLogger(log)
	t.Cleanup(logging.Enable)
	client := redis.NewClient(&redis.Options{
		Addr: "example.invalid:6379", MaxRetries: -1, DialerRetries: 1,
		Dialer: func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("private endpoint") },
	})
	defer func() { _ = client.Close() }()
	if client.Ping(t.Context()).Err() == nil {
		t.Fatal("expected the pool to report a failed dial")
	}
	log.Printf(t.Context(), "private command %q", "secret")
	if got := out.String(); strings.Count(got, "\n") != 1 || !strings.Contains(got, `"category":"connection_pool"`) || strings.Contains(got, "private") || strings.Contains(got, "secret") {
		t.Fatalf("unexpected Redis event: %s", got)
	}
}
