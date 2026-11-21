// Command notify runs the public HTTP API or the Kafka delivery workers.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/api"
	"github.com/EfeDurmaz16/notification-system/internal/delivery"
	"github.com/EfeDurmaz16/notification-system/internal/observability"
	"github.com/EfeDurmaz16/notification-system/internal/store"
	"github.com/EfeDurmaz16/notification-system/internal/stream"
	"golang.org/x/sync/errgroup"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("service stopped", "error", err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) != 1 || args[0] != "api" && args[0] != "worker" {
		return errors.New("usage: notify api|worker")
	}
	mode := args[0]
	shutdown, err := observability.Setup(ctx, "notification-"+mode)
	if err != nil {
		return fmt.Errorf("configure telemetry: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(cleanup); err != nil {
			slog.Warn("flush telemetry", "error", err)
		}
	}()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL must be set")
	}
	// Roll out dual-read workers before enabling channel-routed producers.
	batchRouting := env("BATCH_ROUTING", "shared")
	switch batchRouting {
	case "shared", "dual-read", "channel":
	default:
		return errors.New("BATCH_ROUTING must be shared, dual-read or channel")
	}
	connect, cancel := context.WithTimeout(ctx, 30*time.Second)
	db, err := store.Open(connect, dsn, batchRouting == "channel")
	cancel()
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()
	if mode == "api" {
		return serve(ctx, env("HTTP_ADDR", ":8080"), observability.HTTP(api.New(db)))
	}
	endpoint := os.Getenv("PROVIDER_URL")
	u, err := url.ParseRequestURI(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("PROVIDER_URL must be an absolute HTTP(S) URL")
	}
	idempotent, err := strconv.ParseBool(env("PROVIDER_IDEMPOTENT", "false"))
	if err != nil {
		return errors.New("invalid PROVIDER_IDEMPOTENT")
	}
	window, err := time.ParseDuration(env("PROVIDER_KEY_WINDOW", "24h"))
	if err != nil || window <= 0 {
		return errors.New("invalid PROVIDER_KEY_WINDOW")
	}
	workers, err := strconv.Atoi(env("WORKER_CONCURRENCY", "32"))
	if err != nil || workers < 1 || workers > 512 {
		return errors.New("WORKER_CONCURRENCY must be between 1 and 512")
	}
	batchShare, err := strconv.Atoi(env("BATCH_SHARE_PERCENT", strconv.Itoa(stream.DefaultBatchSharePercent)))
	if err != nil || batchShare < 1 || batchShare > 49 {
		return errors.New("BATCH_SHARE_PERCENT must be between 1 and 49")
	}
	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ",")
	cfg := stream.Config{ChannelReaders: batchRouting != "shared", Brokers: brokers, Group: env("KAFKA_GROUP", "notification-delivery"),
		RedisAddr: env("REDIS_ADDR", "localhost:16379"), Workers: workers, BatchSharePercent: batchShare}
	engine := &delivery.Engine{Store: db, URL: endpoint, Idempotent: idempotent, IdempotencyWindow: window,
		Client: &http.Client{Timeout: 5 * time.Second}}
	recoveryWindow := time.Duration(0)
	if idempotent {
		recoveryWindow = window
	}
	const maxAttempts = 4
	engine.MaxAttempts = maxAttempts
	group, workCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return stream.Run(workCtx, cfg, db, engine) })
	group.Go(func() error {
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-workCtx.Done():
				return workCtx.Err()
			case <-tick.C:
				if err := db.TickWithRecovery(workCtx, 2*time.Minute, recoveryWindow, maxAttempts); err != nil && workCtx.Err() == nil {
					slog.Error("schedule due work", "error", err)
				}
			}
		}
	})
	return group.Wait()
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func serve(ctx context.Context, addr string, handler http.Handler) error {
	srv := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 20 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 90 * time.Second,
		MaxHeaderBytes: 32 << 10}
	stopped := make(chan error, 1)
	go func() { stopped <- srv.ListenAndServe() }()
	slog.Info("HTTP service starting", "address", addr)
	select {
	case err := <-stopped:
		return err
	case <-ctx.Done():
		end, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(end)
	}
}
