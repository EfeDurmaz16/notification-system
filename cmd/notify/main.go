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
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

func main() {
	var level slog.Level
	if err := level.UnmarshalText([]byte(env("LOG_LEVEL", "INFO"))); err != nil {
		fmt.Fprintln(os.Stderr, "invalid LOG_LEVEL")
		os.Exit(1)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))
	redis.SetLogger(&redisDiagnostics{logger: slog.Default()})
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
	modeName := env("BATCH_ROUTING", "shared")
	writes, reads, err := routing(modeName)
	if err != nil {
		return err
	}
	slog.Info("routing configured", "mode", mode, "batch_routing", modeName)
	connect, cancel := context.WithTimeout(ctx, 30*time.Second)
	db, err := store.Open(connect, dsn, writes)
	cancel()
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()
	if mode == "api" {
		limit, err := strconv.Atoi(env("WS_MAX_SUBSCRIPTIONS", strconv.Itoa(api.DefaultSubscriptionLimit)))
		if err != nil || limit < 1 || limit > 1024 {
			return errors.New("WS_MAX_SUBSCRIPTIONS must be between 1 and 1024")
		}
		return serve(ctx, env("HTTP_ADDR", ":8080"), observability.HTTP(api.NewWithSubscriptionLimit(db, limit)))
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
	cfg := stream.Config{ChannelReaders: reads, Brokers: brokers, Group: env("KAFKA_GROUP", "notification-delivery"),
		RedisAddr: env("REDIS_ADDR", "localhost:16379"), Workers: workers, BatchSharePercent: batchShare}
	engine, err := providerEngine(db)
	if err != nil {
		return err
	}
	recoveryWindow := time.Duration(0)
	if engine.Idempotent {
		recoveryWindow = engine.IdempotencyWindow
	}
	group, workCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return serveWorkerHealth(workCtx, env("WORKER_HEALTH_ADDR", "127.0.0.1:8082"), db.Ping, cfg)
	})
	group.Go(func() error { return stream.Run(workCtx, cfg, db, engine) })
	group.Go(func() error {
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		var outage schedulerOutage
		for {
			select {
			case <-workCtx.Done():
				return workCtx.Err()
			case <-tick.C:
				err := db.TickWithRecovery(workCtx, 2*time.Minute, recoveryWindow, engine.MaxAttempts)
				outage.observe(workCtx, time.Now(), err, slog.Default())
			}
		}
	})
	return group.Wait()
}

// Legacy modes keep their published topics and consumer group offsets.
// During rollout a reader can drain both formats before writers switch.
func routing(mode string) (writes, reads store.ChannelRoutes, err error) {
	switch mode {
	case "shared":
	case "dual-read":
		reads.Batch = true
	case "channel":
		writes.Batch, reads.Batch = true, true
	case "read-isolated":
		writes.Batch, reads.Batch, reads.Single = true, true, true
	case "isolated":
		writes = store.ChannelRoutes{Batch: true, Single: true}
		reads = writes
	default:
		err = fmt.Errorf("unknown BATCH_ROUTING %q: expected shared, dual-read, channel, read-isolated or isolated", mode)
	}
	return writes, reads, err
}

func providerEngine(db delivery.Store) (*delivery.Engine, error) {
	endpoint := os.Getenv("PROVIDER_URL")
	u, err := url.ParseRequestURI(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("PROVIDER_URL must be an absolute HTTP(S) URL")
	}
	idempotent, err := strconv.ParseBool(env("PROVIDER_IDEMPOTENT", "false"))
	if err != nil {
		return nil, errors.New("invalid PROVIDER_IDEMPOTENT")
	}
	window, err := time.ParseDuration(env("PROVIDER_KEY_WINDOW", "24h"))
	if err != nil || window <= 0 {
		return nil, errors.New("invalid PROVIDER_KEY_WINDOW")
	}
	attempts, err := strconv.Atoi(env("RETRY_MAX_ATTEMPTS", strconv.Itoa(delivery.DefaultMaxAttempts)))
	if err != nil || attempts < 1 || attempts > 100 {
		return nil, errors.New("RETRY_MAX_ATTEMPTS must be between 1 and 100")
	}
	base, err := time.ParseDuration(env("RETRY_BASE_BACKOFF", delivery.DefaultBaseBackoff.String()))
	if err != nil || base <= 0 || base > time.Minute {
		return nil, errors.New("RETRY_BASE_BACKOFF must be positive and at most 1m")
	}
	// The bundled simulator rejects throttled attempts before creating a receipt.
	// An external URL needs its own explicit guarantee.
	fixtureRejects := endpoint == "http://provider:8081/send"
	throttleRejects, err := strconv.ParseBool(env("PROVIDER_429_NOT_ACCEPTED", strconv.FormatBool(fixtureRejects)))
	if err != nil {
		return nil, errors.New("invalid PROVIDER_429_NOT_ACCEPTED")
	}
	mode := env("PROVIDER_ACK_MODE", "assessment")
	if mode != "assessment" && mode != "message_id" {
		return nil, errors.New("PROVIDER_ACK_MODE must be assessment or message_id")
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	capture := host == "webhook.site" || strings.HasSuffix(host, ".webhook.site")
	endpointAccess, err := strconv.ParseBool(env("PROVIDER_403_404_ENDPOINT", strconv.FormatBool(capture)))
	if err != nil {
		return nil, errors.New("invalid PROVIDER_403_404_ENDPOINT")
	}
	return &delivery.Engine{
		Store: db, URL: endpoint, Idempotent: idempotent, IdempotencyWindow: window,
		MaxAttempts: attempts, BaseBackoff: base, ThrottleRejects: throttleRejects,
		MessageIDReceipt: mode == "message_id", EndpointAccessErrors: endpointAccess,
	}, nil
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
