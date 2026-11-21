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
	"slices"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/delivery"
	dbstore "github.com/EfeDurmaz16/notification-system/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

const DefaultBatchSharePercent = 20

type Config struct {
	ChannelReaders    dbstore.ChannelRoutes
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

var channels = [...]string{"sms", "email", "push"}
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

// Source zero is the original reader for this lane. Indices one through
// three are per-channel readers within the same lane. All feed the same scheduler and pool.
type reader struct {
	route
	source int
}

func (r reader) suffix() string {
	if r.source == 0 {
		return r.route.suffix()
	}
	if r.lane == singleLane {
		return r.route.suffix() + "." + channels[r.source-1]
	}
	return priorities[r.class] + ".batch." + channels[r.source-1]
}

func (r reader) window(workers, batchPercent int) pollWindow {
	if r.source == 0 {
		return r.route.window(workers, batchPercent)
	}
	// Divide the original lane poll budget across the three independent
	// readers. A reader must be able to request at least one record.
	capacity := pollLimit(workers, r.class)
	base := capacity / len(channels)
	if r.source <= capacity%len(channels) {
		base++
	}
	if r.lane == singleLane {
		return newWindow(max(1, base), 100-batchPercent)
	}
	return newWindow(max(1, base), batchPercent)
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
	return newWindow(capacity, share)
}

func newWindow(capacity, share int) pollWindow {
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
	job          delivery.Job
	lease        slot
	refillSource int // source + 1 if this task drains a poll with known backlog
	admitBy      time.Time
	record       *kgo.Record
	result       chan<- error
}

type batch struct {
	route
	source int
	tasks  []task
	more   bool
}

const (
	refillGrace     = 2 * time.Second
	admissionBudget = batchBudget - 40*time.Second
	ageThreshold    = 30 * time.Second
)

type priorityQueue struct {
	lanes       [2][]task
	firstReady  [2]int // first task whose channel can request a permit
	refillUntil [2][1 + len(channels)]time.Time
	lastServed  [2]time.Time
	batchCredit int
}

func (q *priorityQueue) refilling(lane ingressLane, now time.Time) bool {
	for _, until := range q.refillUntil[lane] {
		if until.After(now) {
			return true
		}
	}
	return false
}

func (q *priorityQueue) agingAt(lane ingressLane) time.Time {
	jobs := q.lanes[lane]
	i := q.firstReady[lane]
	if i >= len(jobs) || jobs[i].admitBy.IsZero() {
		return time.Time{}
	}
	start := jobs[i].admitBy.Add(-admissionBudget)
	if q.lastServed[lane].After(start) {
		start = q.lastServed[lane]
	}
	return start.Add(ageThreshold)
}

// Decide without mutating state: a new Kafka poll may arrive before a free
// worker. Credit advances only when an actual task leaves this priority.
func (q *priorityQueue) next(batchPercent int, now time.Time) (lane ingressLane, nextCredit int, ready bool) {
	singleReady := q.firstReady[singleLane] < len(q.lanes[singleLane])
	bulkReady := q.firstReady[bulkLane] < len(q.lanes[bulkLane])
	if !singleReady && (len(q.lanes[singleLane]) > 0 || !q.refilling(singleLane, now)) {
		return bulkLane, 0, bulkReady
	}
	if !bulkReady && (len(q.lanes[bulkLane]) > 0 || !q.refilling(bulkLane, now)) {
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
	if b.more && len(b.tasks) > 0 {
		b.tasks[len(b.tasks)-1].refillSource = b.source + 1
	}
	q.lanes[b.lane] = append(q.lanes[b.lane], b.tasks...)
	q.refillUntil[b.lane][b.source] = time.Time{}
}

func (q *priorityQueue) dispatched(lane ingressLane, credit int, now time.Time) {
	jobs := q.lanes[lane]
	i := q.firstReady[lane]
	refill := jobs[i].refillSource
	// Keep FIFO among eligible jobs and within each parked channel.
	copy(jobs[1:i+1], jobs[:i])
	jobs[0] = task{}
	q.lanes[lane] = jobs[1:]
	q.firstReady[lane] = 0
	q.lastServed[lane] = now
	q.batchCredit = credit
	if refill > 0 {
		// The broker reports more offsets behind this poll. Give its reader
		// time to finish outstanding sends, commit, and refill. A disconnected
		// reader cannot hold its sibling past the grace interval.
		q.refillUntil[lane][refill-1] = now.Add(refillGrace)
	}
}

type quota interface {
	tryAcquire(context.Context, string) (slot, time.Duration, error)
	release(context.Context, slot) error
}

type scheduler struct {
	quota        quota
	blockedUntil map[string]time.Time
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
			kept := slices.DeleteFunc(q.lanes[lane], func(t task) bool {
				if !t.admitBy.IsZero() && !t.admitBy.After(now) {
					t.result <- context.DeadlineExceeded
					return true
				}
				consider(t.admitBy)
				return false
			})
			q.lanes[lane] = kept
			i := 0
			for ; i < len(kept); i++ {
				until := s.blockedUntil[kept[i].job.Channel]
				if !until.After(now) {
					break
				}
				consider(until)
			}
			q.firstReady[lane] = i
			consider(q.agingAt(lane))
			for _, until := range q.refillUntil[lane] {
				consider(until)
			}
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

// A free handler asks for one assignment. A quota denial keeps that handler
// available to any eligible channel, without advancing either fairness cursor.
func (s *scheduler) run(ctx context.Context, in <-chan batch, idle <-chan chan task) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	s.blockedUntil = make(map[string]time.Time, len(channels))
	var worker chan task
	for ctx.Err() == nil {
		now := time.Now()
		wake := s.prepare(now)
		chosen, cursor, credit, ready := s.next(now)
		if worker != nil && ready {
			q := &s.queues[chosen.class]
			t := q.lanes[chosen.lane][q.firstReady[chosen.lane]]
			admission, cancel := context.WithDeadline(ctx, t.admitBy)
			lease, wait, err := s.quota.tryAcquire(admission, t.job.Channel)
			cancel()
			switch {
			case err != nil:
				t.result <- err
			case wait > 0:
				s.blockedUntil[t.job.Channel] = time.Now().Add(wait)
				continue
			default:
				t.lease = lease
				select {
				case worker <- t:
					worker = nil
				case <-ctx.Done():
					release(ctx, s.quota, t)
					return
				}
			}
			q.dispatched(chosen.lane, credit, time.Now())
			s.cursor = cursor
			continue
		}
		var tick <-chan time.Time
		if !wake.IsZero() {
			timer.Reset(time.Until(wake))
			tick = timer.C
		}
		var request <-chan chan task
		if worker == nil {
			request = idle
		}
		select {
		case <-ctx.Done():
			return
		case <-tick:
		case b := <-in:
			s.queues[b.class].enqueue(b)
		case worker = <-request:
		}
	}
}

// Run owns all clients and goroutines until shutdown. Each priority reader
// exposes a limited batch while waiting for the previous batch to commit.
// Priority selects the next locally available job; it does not preempt a POST.
func Run(ctx context.Context, cfg Config, store Store, engine *delivery.Engine) error {
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
	defer func() { _ = rdb.Close() }()
	limit := limiter{client: rdb, prefix: "notifications:quota:"}
	health := providerHealth{limit: limit}
	sender := *engine
	sender.ObserveEndpoint = health.observe
	group, ctx := errgroup.WithContext(ctx)
	input := make(chan batch)
	idle := make(chan chan task)
	group.Go(func() error {
		s := scheduler{batchPercent: cfg.BatchSharePercent, quota: limit}
		s.run(ctx, input, idle)
		return nil
	})
	for range cfg.Workers {
		group.Go(func() error {
			assignment := make(chan task)
			for {
				select {
				case <-ctx.Done():
					return nil
				case idle <- assignment:
				}
				select {
				case <-ctx.Done():
					return nil
				case t := <-assignment:
					t.result <- send(ctx, &sender, limit, t)
				}
			}
		})
	}
	brokerWarnings := &brokerLog{logger: slog.Default()}
	for p := range priorities {
		for _, lane := range [...]ingressLane{bulkLane, singleLane} {
			group.Go(func() error { return consume(ctx, cfg, reader{route: route{p, lane}}, store, input, brokerWarnings) })
			if lane == bulkLane && cfg.ChannelReaders.Batch || lane == singleLane && cfg.ChannelReaders.Single {
				for ch := range channels {
					group.Go(func() error {
						return consume(ctx, cfg, reader{route{p, lane}, ch + 1}, store, input, brokerWarnings)
					})
				}
			}
		}
	}
	return group.Wait()
}

// The immutable job snapshot supplies the channel for quota scheduling. A
// subsequent cancellation still wins until Engine.Send claims the database row.
func load(ctx context.Context, store Store, e event) (delivery.Job, bool, error) {
	job, err := store.GetJob(ctx, e.ID)
	if err != nil {
		return delivery.Job{}, false, fmt.Errorf("load notification %s: %w", e.ID, err)
	}
	if e.Attempt > job.Attempt {
		return delivery.Job{}, false, fmt.Errorf("%w: event=%d row=%d", errFutureGeneration, e.Attempt, job.Attempt)
	}
	return job, e.Attempt == job.Attempt && job.Status == "pending", nil
}

func send(ctx context.Context, sender Sender, limit quota, t task) error {
	defer release(ctx, limit, t)
	if err := ctx.Err(); err != nil {
		return err
	}
	if !time.Now().Before(t.admitBy) {
		return context.DeadlineExceeded
	}
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return sender.Send(callCtx, t.job)
}

func release(ctx context.Context, limit quota, t task) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := limit.release(releaseCtx, t.lease); err != nil {
		slog.Warn("quota release deferred to lease expiry", "id", t.job.ID, "channel", t.job.Channel, "error", err)
	}
}

func consume(ctx context.Context, cfg Config, r reader, store Store, in chan<- batch, brokerWarnings *brokerLog) error {
	topic := "notifications." + r.suffix()
	window := r.window(cfg.Workers, cfg.BatchSharePercent)
	for ctx.Err() == nil {
		client, err := kgo.NewClient(
			kgo.WithHooks(brokerWarnings),
			kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumerGroup(cfg.Group+"."+r.suffix()),
			kgo.ConsumeTopics(topic), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(),
			kgo.FetchMaxWait(25*time.Millisecond), kgo.RebalanceTimeout(2*time.Minute),
		)
		if err != nil {
			return fmt.Errorf("configure %s: %w", topic, err)
		}
		err = consumeBatches(ctx, client, r, store, in, &window)
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

func consumeBatches(ctx context.Context, client *kgo.Client, r reader, store Store, in chan<- batch, window *pollWindow) error {
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
		err := processBatch(ctx, client, r, store, in, records, more)
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
func processBatch(parent context.Context, client *kgo.Client, source reader, store Store, in chan<- batch, records []*kgo.Record, more bool) error {
	deadline := time.Now().Add(batchBudget)
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	results := make(chan error, len(records))
	admitBy := deadline.Add(-40 * time.Second)
	admission, stop := context.WithDeadline(ctx, admitBy)
	defer stop()
	b := batch{route: source.route, source: source.source, more: more}
	for _, r := range records {
		e, err := decode(r.Value)
		if err != nil {
			if err := quarantine(ctx, client, r); err != nil {
				return err
			}
			continue
		}
		job, current, err := load(admission, store, e)
		if errors.Is(err, dbstore.ErrNotFound) || errors.Is(err, errFutureGeneration) {
			if err := quarantine(ctx, client, r); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if current {
			b.tasks = append(b.tasks, task{job: job, record: r, result: results, admitBy: admitBy})
		}
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
		case err := <-results:
			if err != nil && firstErr == nil {
				firstErr = err
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
