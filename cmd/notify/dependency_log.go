package main

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/observability"
)

const dependencyLogInterval = 30 * time.Second

// The scheduler calls observe on its own goroutine. Keep scheduling every
// tick during an outage, but only repeat the error periodically.
type schedulerOutage struct {
	started  time.Time
	lastLog  time.Time
	failures int
}

func (s *schedulerOutage) observe(ctx context.Context, at time.Time, err error, log *slog.Logger) {
	if ctx.Err() != nil {
		return
	}
	if err == nil {
		if s.failures > 0 {
			log.InfoContext(ctx, "scheduler recovered", "failed_ticks", s.failures, "outage_ms", at.Sub(s.started).Milliseconds())
			*s = schedulerOutage{}
		}
		return
	}
	s.failures++
	if s.failures == 1 {
		s.started = at
	} else if at.Sub(s.lastLog) < dependencyLogInterval {
		return
	}
	s.lastLog = at
	log.ErrorContext(ctx, "schedule due work", "failed_ticks", s.failures, observability.StorageError(err))
}

// go-redis printf arguments may contain credentials, commands or server
// replies. Only emit a fixed category and rate-limit duplicate diagnostics.
type redisDiagnostics struct {
	logger  *slog.Logger
	mu      sync.Mutex
	lastLog time.Time
}

func (l *redisDiagnostics) Printf(ctx context.Context, format string, _ ...any) {
	l.mu.Lock()
	if time.Since(l.lastLog) < dependencyLogInterval {
		l.mu.Unlock()
		return
	}
	l.lastLog = time.Now()
	l.mu.Unlock()
	category := "client"
	if strings.HasPrefix(format, "redis: connection pool:") {
		category = "connection_pool"
	}
	l.logger.WarnContext(ctx, "Redis client diagnostic", "category", category)
}
