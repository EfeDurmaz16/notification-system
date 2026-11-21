package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/delivery"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"golang.org/x/sync/errgroup"
)

func TestDecodeReadyEvent(t *testing.T) {
	const value = `{"id":"8d4ad4f2-119a-4bd7-887a-b8c004fed432","attempt":2}`
	quoted, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{[]byte(value), quoted} {
		e, err := decode(data)
		if err != nil || e.Attempt != 2 {
			t.Errorf("%s: %v", data, err)
		}
	}
	for _, data := range []string{"null", "[]", `{"id":"x","attempt":1}`, `{"id":"8d4ad4f2-119a-4bd7-887a-b8c004fed432","attempt":0}`, `{"id":"8d4ad4f2-119a-4bd7-887a-b8c004fed432","attempt":"2"}`} {
		if _, err := decode([]byte(data)); err == nil {
			t.Errorf("accepted invalid event: %s", data)
		}
	}
}

func TestPollLimitTracksWeightAndCapsReplay(t *testing.T) {
	for _, tc := range []struct {
		workers int
		windows [3]int
	}{
		{1, [3]int{6, 3, 1}},
		{24, [3]int{144, 72, 24}},
		{128, [3]int{144, 72, 24}},
	} {
		for class, want := range tc.windows {
			if got := pollLimit(tc.workers, class); got != want {
				t.Errorf("workers=%d class=%d got %d want %d", tc.workers, class, got, want)
			}
		}
	}
}

type openQuota struct{}

func (openQuota) tryAcquire(context.Context, string) (slot, time.Duration, error) {
	return slot{}, 0, nil
}
func (openQuota) release(context.Context, slot) error { return nil }

func take(idle chan<- chan task) task {
	worker := make(chan task)
	idle <- worker
	return <-worker
}

func TestWeightedDispatch(t *testing.T) {
	for _, tc := range []struct {
		name              string
		backlog, expected [3]int
		n                 int
	}{
		{"all saturated", [3]int{100, 100, 100}, [3]int{36, 18, 6}, 60},
		{"unused capacity", [3]int{10, 20, 100}, [3]int{10, 20, 30}, 60},
		{"only low", [3]int{0, 0, 100}, [3]int{0, 0, 60}, 60},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			in := make(chan batch)
			idle := make(chan chan task)
			done := make(chan struct{})
			go func() { defer close(done); s := scheduler{quota: openQuota{}}; s.run(ctx, in, idle) }()
			defer func() { cancel(); <-done }()
			for class, n := range tc.backlog {
				jobs := make([]task, n)
				for i := range jobs {
					jobs[i].job.Attempt = class
				}
				in <- batch{route: route{class, bulkLane}, tasks: jobs}
			}
			var got [3]int
			for range tc.n {
				job := take(idle)
				got[job.job.Attempt]++
			}
			if got != tc.expected {
				t.Errorf("got %v, want %v", got, tc.expected)
			}
		})
	}
}

type testJobs struct{ job delivery.Job }

func (s testJobs) GetJob(context.Context, string) (delivery.Job, error) { return s.job, nil }

type sendFunc func(context.Context, delivery.Job) error

func (f sendFunc) Send(ctx context.Context, j delivery.Job) error { return f(ctx, j) }

func TestOnlyCurrentPendingEventMayRequestQuota(t *testing.T) {
	for _, tc := range []struct {
		state        string
		eventAttempt int
		want         bool
	}{
		{"pending", 2, true}, {"pending", 1, false}, {"cancelled", 2, false},
		{"sending", 2, false}, {"accepted", 2, false}, {"failed", 2, false},
		{"unknown", 2, false}, {"retry_wait", 2, false}, {"scheduled", 2, false},
	} {
		store := testJobs{delivery.Job{ID: "example", Status: tc.state, Attempt: 2}}
		_, current, err := load(t.Context(), store, event{ID: "example", Attempt: tc.eventAttempt})
		if err != nil || current != tc.want {
			t.Errorf("state %s event %d: current=%t, err=%v", tc.state, tc.eventAttempt, current, err)
		}
	}
}

func TestQuotaBackendFailureBlocksSend(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0", MaxRetries: -1, DialTimeout: 50 * time.Millisecond})
	defer func() { _ = client.Close() }()
	job := delivery.Job{ID: "a", Channel: "sms", Attempt: 1}
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	idle := make(chan chan task, 1)
	handler := make(chan task)
	idle <- handler
	results := make(chan error, 1)
	s := scheduler{quota: limiter{client: client, prefix: "test:"}}
	s.queues[0].enqueue(batch{tasks: []task{{job: job, result: results, admitBy: time.Now().Add(time.Second)}}})
	go s.run(ctx, nil, idle)
	select {
	case <-handler:
		t.Fatal("sent without Redis quota")
	case out := <-results:
		if out == nil {
			t.Fatal("expected quota backend error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no quota result")
	}
}

// Two independent Redis clients share one channel ledger, as worker replicas do.
// Delay some requests after admission; verify every observed send-time window.
func TestRedisRollingLimitAcrossReplicas(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR to verify the real Redis Lua path")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("test:notify:%d:", time.Now().UnixNano())
	var replicas [2]limiter
	for i := range replicas {
		c := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1})
		t.Cleanup(func() { _ = c.Close() })
		replicas[i] = limiter{client: c, prefix: prefix}
	}
	const n = 140
	starts := make([]time.Time, n)
	var g errgroup.Group
	g.SetLimit(12)
	for i := range n {
		g.Go(func() error {
			lim := replicas[i%2]
			var permit slot
			for {
				lease, wait, err := lim.tryAcquire(ctx, "sms")
				if err != nil {
					return err
				}
				if wait == 0 {
					permit = lease
					break
				}
				if err := sleep(ctx, wait); err != nil {
					return err
				}
			}
			if i%5 == 0 {
				time.Sleep(25 * time.Millisecond)
			}
			starts[i] = time.Now()
			return lim.release(ctx, permit)
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(starts, func(a, b time.Time) int { return a.Compare(b) })
	peak, left := 0, 0
	for i, s := range starts {
		for s.Sub(starts[left]) >= time.Second {
			left++
		}
		peak = max(peak, i-left+1)
	}
	t.Logf("replicas=2 dispatches=%d maximum_observed_in_rolling_second=%d elapsed=%v", n, peak, starts[n-1].Sub(starts[0]))
	if peak > 100 {
		t.Fatal("channel quota exceeded")
	}
}

// Exercise the real broker: a failed batch must be presented again to a group
// member with the same group ID. The topic is unique and deleted on success.
func TestKafkaRedeliveryAfterFailedBatch(t *testing.T) {
	broker := os.Getenv("TEST_KAFKA_BROKER")
	if broker == "" {
		t.Skip("set TEST_KAFKA_BROKER to test real broker replay")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	topic := fmt.Sprintf("test-notify-%d", time.Now().UnixNano())
	admin, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	create := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = topic
	ct.NumPartitions = 1
	ct.ReplicationFactor = 1
	create.Topics = []kmsg.CreateTopicsRequestTopic{ct}
	resp, err := create.RequestWith(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create topic: %+v", resp.Topics)
	}
	defer func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer dcancel()
		del := kmsg.NewPtrDeleteTopicsRequest()
		del.TopicNames = []string{topic}
		del.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &topic}}
		if _, err := del.RequestWith(dctx, admin); err != nil {
			t.Errorf("delete topic: %v", err)
		}
	}()
	const value = `{"id":"8d4ad4f2-119a-4bd7-887a-b8c004fed432","attempt":1}`
	// Two offsets in one partition. Ack the higher before failing the lower.
	produce := func(n int) {
		t.Helper()
		var records []*kgo.Record
		for range n {
			records = append(records, &kgo.Record{Topic: topic, Value: []byte(value)})
		}
		if err := admin.ProduceSync(ctx, records...).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	produce(2)
	failed := errors.New("temporary DB error before offset commit")
	for stage := range 3 {
		consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic), kgo.ConsumerGroup(topic), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer consumer.CloseAllowingRebalance()
			pollCtx, stop := context.WithCancel(ctx)
			defer stop()
			in := make(chan batch)
			result := make(chan error, 1)
			go func() {
				result <- consumeBatches(pollCtx, consumer, reader{}, testJobs{delivery.Job{Channel: "sms", Status: "pending", Attempt: 1}}, in, &pollWindow{size: 8, cap: 8})
			}()
			read := func() []task {
				t.Helper()
				select {
				case b := <-in:
					return b.tasks
				case err := <-result:
					t.Fatalf("poll %d: %v", stage, err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				return nil
			}
			items := read()
			switch stage {
			case 0, 1:
				if len(items) != 2 || items[0].record.Offset != 0 || items[1].record.Offset != 1 {
					t.Fatalf("stage %d: expected both offsets, got %+v", stage, items)
				}
				items[1].result <- nil
				var lower error
				if stage == 0 {
					lower = failed
				}
				items[0].result <- lower
				if stage == 1 {
					// Seeing the next poll demonstrates the batch commit finished.
					produce(1)
					items = read()
					if len(items) != 1 || items[0].record.Offset != 2 {
						t.Fatalf("stage %d: expected sentinel offset 2", stage)
					}
					items[0].result <- failed
				}
			case 2:
				if len(items) != 1 || items[0].record.Offset != 2 {
					t.Fatal("successful earlier batch was not committed")
				}
				items[0].result <- failed
			}
			if err := <-result; !errors.Is(err, failed) {
				t.Errorf("got %v, want temporary failure", err)
			}
		}()
	}
	t.Log("lower offset failure replays offsets 0 and 1; after durable success the next member resumes at offset 2")
}

// Observe an actual quota denial before introducing another channel. The
// second job must run on the very handler that was ready for the first.
type observedQuota struct {
	quota
	waiting chan struct{}
}

func (q observedQuota) tryAcquire(ctx context.Context, channel string) (slot, time.Duration, error) {
	lease, wait, err := q.quota.tryAcquire(ctx, channel)
	if wait > 0 {
		select {
		case q.waiting <- struct{}{}:
		default:
		}
	}
	return lease, wait, err
}

func TestRedisSMSBacklogLeavesHandlerAvailableForEmail(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR to verify channel isolation with real Redis")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1})
	defer func() { _ = client.Close() }()
	limit := limiter{client: client, prefix: fmt.Sprintf("test:isolated:%d:", time.Now().UnixNano())}
	// Hold the SMS pacing gate far past the test deadline. Redis still has
	// capacity for email. Clearing this unique key simulates SMS recovery.
	smsGate := limit.prefix + "{sms}:next"
	stamp, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, smsGate, stamp.Add(time.Minute).UnixMicro(), time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Del(context.WithoutCancel(ctx), smsGate).Err() }()
	q := observedQuota{quota: limit, waiting: make(chan struct{}, 1)}
	s := scheduler{batchPercent: 20, quota: q}
	in := make(chan batch)
	idle := make(chan chan task, 1)
	handler := make(chan task)
	idle <- handler
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		s.run(ctx, in, idle)
	}()
	defer func() { cancel(); <-stopped }()
	results := make(chan error, 3)
	deadline, _ := ctx.Deadline()
	job := func(id, channel string) task {
		return task{job: delivery.Job{ID: id, Channel: channel, Attempt: 1}, admitBy: deadline, result: results}
	}
	in <- batch{route: route{1, bulkLane}, tasks: []task{job("sms-1", "sms"), job("sms-2", "sms")}}
	select {
	case <-q.waiting:
	case <-ctx.Done():
		t.Fatal("SMS did not encounter the quota gate")
	}
	in <- batch{route: route{1, bulkLane}, tasks: []task{job("email-1", "email")}}
	var sent []string
	provider := sendFunc(func(_ context.Context, job delivery.Job) error {
		sent = append(sent, job.ID)
		return nil
	})
	runOne := func(want string) {
		t.Helper()
		select {
		case item := <-handler:
			if item.job.ID != want {
				t.Fatalf("handler got %s, want %s", item.job.ID, want)
			}
			if err := send(ctx, provider, limit, item); err != nil {
				t.Fatal(err)
			}
		case failed := <-results:
			t.Fatalf("unexpected admission failure: %v", failed)
		case <-ctx.Done():
			t.Fatalf("handler did not receive %s", want)
		}
	}
	runOne("email-1")
	if err := client.Del(ctx, smsGate).Err(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"sms-1", "sms-2"} {
		idle <- handler
		runOne(id)
	}
	if !slices.Equal(sent, []string{"email-1", "sms-1", "sms-2"}) {
		t.Fatalf("provider calls %v", sent)
	}
}
