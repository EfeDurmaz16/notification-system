// Package stream dispatches durable ready events through a shared, rate-limited
// worker pool. The notification row arbitrates duplicate deliveries and cancel.
package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/delivery"
	dbstore "github.com/EfeDurmaz16/notification-system/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

type Config struct {
	Brokers   []string
	Group     string
	RedisAddr string
	Workers   int
}

type Store interface {
	GetJob(context.Context, string) (delivery.Job, error)
}

type Sender interface {
	Send(context.Context, delivery.Job) error
}

var errFutureGeneration = errors.New("ready event is ahead of the committed notification")

var validID = regexp.MustCompile("(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

var priorities = [...]string{"high", "normal", "low"}
var weighted = [...]int{0, 0, 0, 0, 0, 0, 1, 1, 1, 2}

type event struct {
	ID      string `json:"id"`
	Attempt int    `json:"attempt"`
}

func decode(data []byte) (event, error) {
	// Debezium's expand.json.payload setting controls whether the event is an
	// object or a JSON-encoded string after the schemaless JSON converter.
	if len(data) > 0 && data[0] == '"' {
		var object string
		if err := json.Unmarshal(data, &object); err != nil {
			return event{}, err
		}
		data = []byte(object)
	}
	var e event
	if err := json.Unmarshal(data, &e); err != nil {
		return event{}, err
	}
	if !validID.MatchString(e.ID) || e.Attempt < 1 {
		return event{}, errors.New("invalid ready event ID or attempt")
	}
	return e, nil
}

const batchBudget = 105 * time.Second

// Keep the low-only window large enough to fill the pool at the default
// concurrency. The upper bound limits replay exposure during a rebalance.
func pollLimit(workers, class int) int {
	places := 0
	for _, p := range weighted {
		if p == class {
			places++
		}
	}
	return places * min(workers, 24)
}

type task struct {
	admitBy time.Time
	event   event
	record  *kgo.Record
	result  chan<- outcome
}
type outcome struct {
	record *kgo.Record
	err    error
}

type batch struct {
	class int
	tasks []task
}

type scheduler struct {
	queues [3][]task
	cursor int
}

func (s *scheduler) next() (class, index int, available bool) {
	for n := range len(weighted) {
		i := (s.cursor + n) % len(weighted)
		if class := weighted[i]; len(s.queues[class]) > 0 {
			return class, i, true
		}
	}
	return 0, 0, false
}

func (s *scheduler) run(ctx context.Context, in <-chan batch, out chan<- task) {
	for {
		var ready chan<- task
		var selected task
		class, i, ok := s.next()
		if ok {
			ready = out
			selected = s.queues[class][0]
		}
		select {
		case <-ctx.Done():
			return
		case b := <-in:
			s.queues[b.class] = append(s.queues[b.class], b.tasks...)
		case ready <- selected:
			s.queues[class][0] = task{}
			s.queues[class] = s.queues[class][1:]
			s.cursor = (i + 1) % len(weighted)
		}
	}
}

// Run owns all clients and goroutines until shutdown. Each priority reader
// exposes a limited batch while waiting for the previous batch to commit.
// Priority selects the next locally available job; it does not preempt a POST.
func Run(ctx context.Context, cfg Config, store Store, sender Sender) error {
	if len(cfg.Brokers) == 0 || cfg.Group == "" || cfg.RedisAddr == "" || cfg.Workers < 1 {
		return errors.New("missing stream brokers, group, Redis address or workers")
	}
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, MaxRetries: -1, ContextTimeoutEnabled: true})
	defer rdb.Close()
	limit := limiter{client: rdb, prefix: "notifications:quota:"}
	group, ctx := errgroup.WithContext(ctx)
	input := make(chan batch)
	ready := make(chan task)
	group.Go(func() error { var s scheduler; s.run(ctx, input, ready); return nil })
	for range cfg.Workers {
		group.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case t := <-ready:
					t.result <- outcome{record: t.record, err: process(ctx, store, sender, limit, t.event, t.admitBy)}
				}
			}
		})
	}
	for p := range priorities {
		group.Go(func() error { return consume(ctx, cfg, p, input) })
	}
	return group.Wait()
}

func process(ctx context.Context, store Store, sender Sender, limit limiter, e event, admitBy time.Time) error {
	admissionCtx, cancelAdmission := context.WithDeadline(ctx, admitBy)
	defer cancelAdmission()
	job, err := store.GetJob(admissionCtx, e.ID)
	if err != nil {
		return fmt.Errorf("load notification %s: %w", e.ID, err)
	}
	if e.Attempt < job.Attempt {
		return nil
	}
	if e.Attempt > job.Attempt {
		return fmt.Errorf("%w: event=%d row=%d", errFutureGeneration, e.Attempt, job.Attempt)
	}
	lease, err := limit.acquire(admissionCtx, job.Channel)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = sender.Send(callCtx, job)
	// On shutdown keep the reservation conservative if Redis is unreachable.
	releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer releaseCancel()
	if releaseErr := limit.release(releaseCtx, lease); releaseErr != nil {
		slog.Warn("quota release deferred to lease expiry", "id", job.ID, "channel", job.Channel, "error", releaseErr)
	}
	return err
}

func consume(ctx context.Context, cfg Config, class int, in chan<- batch) error {
	topic := "notifications." + priorities[class]
	for ctx.Err() == nil {
		client, err := kgo.NewClient(
			kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumerGroup(cfg.Group+"."+priorities[class]),
			kgo.ConsumeTopics(topic), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(),
			kgo.FetchMaxWait(25*time.Millisecond), kgo.RebalanceTimeout(2*time.Minute),
		)
		if err != nil {
			return fmt.Errorf("configure %s: %w", topic, err)
		}
		err = consumeBatches(ctx, client, class, in, pollLimit(cfg.Workers, class))
		client.CloseAllowingRebalance()
		if ctx.Err() != nil {
			return nil
		}
		slog.Error("consumer restarting from committed offsets", "topic", topic, "error", err)
		if err := sleep(ctx, time.Second); err != nil {
			return nil
		}
	}
	return nil
}

func consumeBatches(ctx context.Context, client *kgo.Client, class int, in chan<- batch, batchSize int) error {
	for {
		fetches := client.PollRecords(ctx, batchSize)
		if errs := fetches.Errors(); len(errs) > 0 {
			return errs[0].Err
		}
		records := fetches.Records()
		if len(records) == 0 {
			client.AllowRebalance()
			continue
		}
		err := processBatch(ctx, client, class, in, records)
		if err != nil {
			return err
		}
		client.AllowRebalance()
	}
}

// Bound time spent holding a group generation. Stop new dispatch admissions
// early enough to finish the bounded provider call before the commit deadline.
func processBatch(parent context.Context, client *kgo.Client, class int, in chan<- batch, records []*kgo.Record) error {
	deadline := time.Now().Add(batchBudget)
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	results := make(chan outcome, len(records))
	b := batch{class: class}
	for _, r := range records {
		e, err := decode(r.Value)
		if err != nil {
			if err := quarantine(ctx, client, r); err != nil {
				return err
			}
			continue
		}
		b.tasks = append(b.tasks, task{event: e, record: r, result: results, admitBy: deadline.Add(-40 * time.Second)})
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case in <- b:
	}
	var firstErr error
	for range b.tasks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-results:
			if errors.Is(result.err, dbstore.ErrNotFound) || errors.Is(result.err, errFutureGeneration) {
				result.err = quarantine(ctx, client, result.record)
			}
			if result.err != nil && firstErr == nil {
				firstErr = result.err
			}
		}
	}
	if firstErr != nil {
		return firstErr
	}
	// Every fetched offset is either durably processed or quarantined.
	// Never commit past a still-running lower offset in the same partition.
	if err := client.CommitUncommittedOffsets(ctx); err != nil {
		return err
	}
	return nil
}

func quarantine(ctx context.Context, client *kgo.Client, r *kgo.Record) error {
	// A malformed record can contain recipient data. Store its coordinates to
	// support inspection in the source log without copying raw content.
	ref := fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset)
	record := &kgo.Record{Topic: "notifications.invalid", Key: []byte(ref), Value: []byte(ref)}
	qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.ProduceSync(qctx, record).FirstErr(); err != nil {
		return fmt.Errorf("quarantine %s: %w", ref, err)
	}
	slog.Warn("quarantined malformed ready event", "source", ref)
	return nil
}
