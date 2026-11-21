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

const DefaultBatchSharePercent = 20

type Config struct {
	BatchSharePercent int
	Brokers           []string
	Group             string
	RedisAddr         string
	Workers           int
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

type ingressLane int

const (
	bulkLane ingressLane = iota
	singleLane
)

type route struct {
	class int
	lane  ingressLane
}

func (r route) suffix() string {
	name := priorities[r.class]
	if r.lane == singleLane {
		return name + ".single"
	}
	return name
}

// The per-reader cap limits replay exposure. A lone lane can use the full
// priority window once its polls show that workers are draining it quickly.
func pollLimit(workers, class int) int {
	places := 0
	for _, p := range weighted {
		if p == class {
			places++
		}
	}
	return places * min(workers, 24)
}

type pollWindow struct {
	size int
	cap  int
}

func (r route) window(workers, batchPercent int) pollWindow {
	capacity := pollLimit(workers, r.class)
	share := batchPercent
	if r.lane == singleLane {
		share = 100 - share
	}
	return pollWindow{size: max(1, capacity*share/100), cap: capacity}
}

func (w *pollWindow) observe(records int, elapsed time.Duration) {
	// Aim well inside the 65s admission budget. If a small-share reader
	// drains slowly, holding fewer offsets avoids replaying the whole poll.
	const target = 8 * time.Second
	switch {
	case elapsed > target:
		w.size = max(1, min(w.size, int(time.Duration(records)*target/elapsed)))
	case records == w.size && elapsed < target/2:
		w.size = min(w.cap, w.size*2)
	}
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
	route
	tasks []task
	more  bool
}

const (
	refillGrace     = 2 * time.Second
	admissionBudget = batchBudget - 40*time.Second
	ageThreshold    = 30 * time.Second
)

type priorityQueue struct {
	lanes       [2][]task
	more        [2]bool
	refillUntil [2]time.Time
	lastServed  [2]time.Time
	batchCredit int
}

func (q *priorityQueue) agingAt(lane ingressLane) time.Time {
	jobs := q.lanes[lane]
	if len(jobs) == 0 || jobs[0].admitBy.IsZero() {
		return time.Time{}
	}
	start := jobs[0].admitBy.Add(-admissionBudget)
	if q.lastServed[lane].After(start) {
		start = q.lastServed[lane]
	}
	return start.Add(ageThreshold)
}

// Decide without mutating state: a new Kafka poll may arrive before a free
// worker. Credit advances only when an actual task leaves this priority.
func (q *priorityQueue) next(batchPercent int, now time.Time) (lane ingressLane, nextCredit int, ready bool) {
	singleReady, bulkReady := len(q.lanes[singleLane]) != 0, len(q.lanes[bulkLane]) != 0
	if !singleReady && !q.refillUntil[singleLane].After(now) {
		return bulkLane, 0, bulkReady
	}
	if !bulkReady && !q.refillUntil[bulkLane].After(now) {
		return singleLane, 0, singleReady
	}
	if singleReady && bulkReady {
		// At very low provider throughput, waiting 100 class opportunities
		// for a 1% batch share can outlast a healthy Kafka poll's deadline.
		bulkAge, singleAge := q.agingAt(bulkLane), q.agingAt(singleLane)
		if !bulkAge.IsZero() && !bulkAge.After(now) && (singleAge.IsZero() || bulkAge.Before(singleAge)) {
			return bulkLane, 0, true
		}
		if !singleAge.IsZero() && !singleAge.After(now) {
			return singleLane, q.batchCredit, true
		}
	}

	credit := q.batchCredit + batchPercent
	if credit >= 100 {
		return bulkLane, credit - 100, bulkReady
	}
	return singleLane, credit, singleReady
}

func (q *priorityQueue) enqueue(b batch) {
	q.lanes[b.lane] = append(q.lanes[b.lane], b.tasks...)
	q.more[b.lane] = b.more
	q.refillUntil[b.lane] = time.Time{}
}

func (q *priorityQueue) dispatched(lane ingressLane, credit int, now time.Time) {
	jobs := q.lanes[lane]
	jobs[0] = task{}
	q.lanes[lane] = jobs[1:]
	q.lastServed[lane] = now
	q.batchCredit = credit
	if len(jobs) == 1 && q.more[lane] {
		// The broker reports more offsets behind this poll. Give its reader
		// time to finish outstanding sends, commit, and refill. A disconnected
		// reader cannot hold its sibling past the grace interval.
		q.refillUntil[lane] = now.Add(refillGrace)
	}
}

type scheduler struct {
	queues       [len(priorities)]priorityQueue
	cursor       int
	batchPercent int
}

// Expired tasks must replay without spending a worker slot or advancing any
// fairness cursor. Wake for refill/aging/deadline transitions even if no new
// poll arrives while the scheduler is waiting for an available worker.
func (s *scheduler) prepare(now time.Time) time.Time {
	var wake time.Time
	consider := func(at time.Time) {
		if at.After(now) && (wake.IsZero() || at.Before(wake)) {
			wake = at
		}
	}
	for p := range s.queues {
		q := &s.queues[p]
		for _, lane := range [...]ingressLane{singleLane, bulkLane} {
			jobs := q.lanes[lane]
			for len(jobs) > 0 && !jobs[0].admitBy.IsZero() && !jobs[0].admitBy.After(now) {
				t := jobs[0]
				t.result <- outcome{record: t.record, err: context.DeadlineExceeded}
				jobs[0] = task{}
				jobs = jobs[1:]
			}
			q.lanes[lane] = jobs
			if len(jobs) > 0 {
				consider(jobs[0].admitBy)
				consider(q.agingAt(lane))
			}
			consider(q.refillUntil[lane])
		}
	}
	return wake
}

func (s *scheduler) next(now time.Time) (chosen route, cursor, credit int, ready bool) {
	for n := range len(weighted) {
		i := (s.cursor + n) % len(weighted)
		class := weighted[i]
		lane, nextCredit, ok := s.queues[class].next(s.batchPercent, now)
		if ok {
			return route{class, lane}, (i + 1) % len(weighted), nextCredit, true
		}
	}
	return route{}, 0, 0, false
}

func (s *scheduler) run(ctx context.Context, in <-chan batch, out chan<- task) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for ctx.Err() == nil {
		now := time.Now()
		var tick <-chan time.Time
		if wake := s.prepare(now); !wake.IsZero() {
			timer.Reset(time.Until(wake))
			tick = timer.C
		}
		var ready chan<- task
		var selected task
		chosen, cursor, credit, ok := s.next(now)
		if ok {
			ready = out
			selected = s.queues[chosen.class].lanes[chosen.lane][0]
		}
		select {
		case <-ctx.Done():
			return
		case <-tick:
		case b := <-in:
			s.queues[b.class].enqueue(b)
		case ready <- selected:
			s.queues[chosen.class].dispatched(chosen.lane, credit, time.Now())
			s.cursor = cursor
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
	if cfg.BatchSharePercent == 0 {
		cfg.BatchSharePercent = DefaultBatchSharePercent
	}
	if cfg.BatchSharePercent < 1 || cfg.BatchSharePercent > 49 {
		return errors.New("batch dispatch share must be from 1 to 49 percent")
	}
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, MaxRetries: -1, ContextTimeoutEnabled: true})
	defer rdb.Close()
	limit := limiter{client: rdb, prefix: "notifications:quota:"}
	group, ctx := errgroup.WithContext(ctx)
	input := make(chan batch)
	ready := make(chan task)
	group.Go(func() error {
		s := scheduler{batchPercent: cfg.BatchSharePercent}
		s.run(ctx, input, ready)
		return nil
	})
	for range cfg.Workers {
		group.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case t := <-ready:
					// Shutdown and a handoff can become ready together. Leave
					// that unstarted offset for the next group member.
					if ctx.Err() != nil {
						return nil
					}
					t.result <- outcome{record: t.record, err: process(ctx, store, sender, limit, t.event, t.admitBy)}
				}
			}
		})
	}
	for p := range priorities {
		for _, lane := range [...]ingressLane{bulkLane, singleLane} {
			group.Go(func() error { return consume(ctx, cfg, route{p, lane}, input) })
		}
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

func consume(ctx context.Context, cfg Config, r route, in chan<- batch) error {
	topic := "notifications." + r.suffix()
	window := r.window(cfg.Workers, cfg.BatchSharePercent)
	for ctx.Err() == nil {
		client, err := kgo.NewClient(
			kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumerGroup(cfg.Group+"."+r.suffix()),
			kgo.ConsumeTopics(topic), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(),
			kgo.FetchMaxWait(25*time.Millisecond), kgo.RebalanceTimeout(2*time.Minute),
		)
		if err != nil {
			return fmt.Errorf("configure %s: %w", topic, err)
		}
		err = consumeBatches(ctx, client, r, in, &window)
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

func consumeBatches(ctx context.Context, client *kgo.Client, r route, in chan<- batch, window *pollWindow) error {
	for {
		fetches := client.PollRecords(ctx, window.size)
		if errs := fetches.Errors(); len(errs) > 0 {
			return errs[0].Err
		}
		records := fetches.Records()
		if len(records) == 0 {
			client.AllowRebalance()
			continue
		}
		start := time.Now()
		more := false
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if n := len(p.Records); n > 0 && p.Records[n-1].Offset+1 < p.HighWatermark {
				more = true
			}
		})
		err := processBatch(ctx, client, r, in, records, more)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// Retry a single offset first when the admission window expires.
				// Keep this window across reconnects to avoid replaying a large
				// group of already-processed offsets on each deadline.
				window.size = 1
			}
			return err
		}
		window.observe(len(records), time.Since(start))
		client.AllowRebalance()
	}
}

// Bound time spent holding a group generation. Stop new dispatch admissions
// early enough to finish the bounded provider call before the commit deadline.
func processBatch(parent context.Context, client *kgo.Client, source route, in chan<- batch, records []*kgo.Record, more bool) error {
	deadline := time.Now().Add(batchBudget)
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	results := make(chan outcome, len(records))
	b := batch{route: source, more: more}
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
