package main

import (
	"context"
	"net/http"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/stream"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

type workerProbe struct {
	name string
	ping func(context.Context) error
}

func serveWorkerHealth(ctx context.Context, addr string, pingDB func(context.Context) error, cfg stream.Config) error {
	// Probe clients stay open across checks and never join a consumer group or
	// acquire a channel permit. The database probe shares the worker's pool.
	broker, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		return err
	}
	defer broker.Close()
	cache := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, MaxRetries: -1, ContextTimeoutEnabled: true})
	defer func() { _ = cache.Close() }()
	return serve(ctx, addr, workerHealth(ctx,
		workerProbe{"postgres", pingDB},
		workerProbe{"redis", func(ctx context.Context) error { return cache.Ping(ctx).Err() }},
		workerProbe{"kafka", broker.Ping},
	))
}

func workerHealth(workerCtx context.Context, probes ...workerProbe) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		for _, probe := range probes {
			if workerCtx.Err() != nil {
				http.Error(w, "worker draining", http.StatusServiceUnavailable)
				return
			}
			if err := probe.ping(ctx); err != nil {
				http.Error(w, probe.name+" unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		if workerCtx.Err() != nil {
			http.Error(w, "worker draining", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
