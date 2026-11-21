package stream

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/delivery"
)

func TestIngressShareUnderContinuousContention(t *testing.T) {
	for _, percent := range []int{1, 10, 20, 33, 49} {
		t.Run(strconv.Itoa(percent), func(t *testing.T) {
			var q priorityQueue
			q.lanes[singleLane], q.lanes[bulkLane] = []task{{}}, []task{{}}
			bulk := 0
			for slot := 1; slot <= 200; slot++ {
				lane, credit, ok := q.next(percent, time.Now())
				if !ok {
					t.Fatal("two ready lanes were skipped")
				}
				if lane == bulkLane {
					bulk++
				}
				if want := slot * percent / 100; bulk != want {
					t.Fatalf("after %d dispatches: %d batch, want %d", slot, bulk, want)
				}
				q.batchCredit = credit
			}
		})
	}
}

func TestShortSingleArrivalsCannotStarveReadyBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	in, idle := make(chan batch), make(chan chan task)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s := scheduler{batchPercent: 20, quota: openQuota{}}
		s.run(ctx, in, idle)
	}()
	defer func() { cancel(); <-done }()
	in <- batch{route: route{2, bulkLane}, tasks: make([]task, 30)}
	// A bulk-only period must not accrue a claim on future dispatches.
	for range 3 {
		take(idle)
	}
	singles := 0
	for slot := 1; slot <= 15; slot++ {
		if singles == 0 {
			in <- batch{route: route{2, singleLane}, tasks: []task{{job: delivery.Job{ID: "single"}}}}
			singles++
		}
		job := take(idle)
		gotBulk := job.job.ID == ""
		if gotBulk != (slot%5 == 0) {
			t.Fatalf("dispatch %d: bulk=%t", slot, gotBulk)
		}
		if !gotBulk {
			singles--
		}
	}
}

func TestIngressTopicAndGroupSuffixes(t *testing.T) {
	for i, tier := range []string{"high", "normal", "low"} {
		if (route{i, bulkLane}).suffix() != tier || (route{i, singleLane}).suffix() != tier+".single" {
			t.Fatalf("route mismatch for %s", tier)
		}
	}
}

func TestPriorityAndIngressBudgetsCompose(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	in, idle := make(chan batch), make(chan chan task)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s := scheduler{batchPercent: 20, quota: openQuota{}}
		s.run(ctx, in, idle)
	}()
	defer func() { cancel(); <-done }()
	for p := range priorities {
		for _, lane := range []ingressLane{singleLane, bulkLane} {
			jobs := make([]task, 100)
			for i := range jobs {
				jobs[i].job = delivery.Job{ID: "single", Attempt: p}
				if lane == bulkLane {
					jobs[i].job.ID = "bulk"
				}
			}
			in <- batch{route: route{p, lane}, tasks: jobs}
		}
	}
	var totals, bulk [3]int
	for range 100 {
		e := take(idle).job
		totals[e.Attempt]++
		if e.ID == "bulk" {
			bulk[e.Attempt]++
		}
	}
	if totals != [3]int{60, 30, 10} || bulk != [3]int{12, 6, 2} {
		t.Fatalf("priority totals %v; bulk within priority %v", totals, bulk)
	}
}

func TestPollWindowCanRecoverLoneLaneCapacity(t *testing.T) {
	w := (route{2, bulkLane}).window(24, 1)
	if w.size != 1 || w.cap != 24 {
		t.Fatalf("initial 1%% low lane window %+v", w)
	}
	for i := 0; i < 10; i++ {
		w.observe(w.size, time.Second)
	}
	if w.size != 24 {
		t.Fatalf("lone lane did not grow to reader cap: %+v", w)
	}
	w.observe(24, 48*time.Second)
	if w.size != 4 {
		t.Fatalf("slow lane retained excessive window: %+v", w)
	}
	w.observe(1, 20*time.Second)
	if w.size != 1 {
		t.Fatalf("tiny lane must keep polling: %+v", w)
	}
	w.observe(1, 8*time.Second)
	if w.size != 1 {
		t.Fatalf("steady lane must keep current window: %+v", w)
	}
}

func TestRunRejectsUnsupportedBatchShare(t *testing.T) {
	for _, percent := range []int{-1, 50, 100} {
		cfg := Config{Brokers: []string{"broker"}, Group: "delivery", RedisAddr: "redis", Workers: 1, BatchSharePercent: percent}
		if err := Run(t.Context(), cfg, nil, nil); err == nil {
			t.Fatalf("accepted %d%%", percent)
		}
	}
}

func TestKnownKafkaBacklogRetainsTurnOnlyThroughRefillGrace(t *testing.T) {
	now := time.Unix(2000, 0)
	s := scheduler{batchPercent: 20, quota: openQuota{}}
	q := &s.queues[0]
	q.enqueue(batch{route: route{0, singleLane}, tasks: []task{{}}, more: true})
	q.enqueue(batch{route: route{0, bulkLane}, tasks: []task{{}}})
	q.dispatched(singleLane, 20, now)
	if _, _, ok := q.next(20, now.Add(time.Millisecond)); ok {
		t.Fatal("busy reader donated a single turn before it could refill")
	}
	s.queues[1].enqueue(batch{route: route{1, bulkLane}, tasks: []task{{}}})
	r, _, _, ok := s.next(now.Add(time.Millisecond))
	if !ok || r != (route{1, bulkLane}) {
		t.Fatalf("unrelated priority stalled during refill: %+v", r)
	}
	if wake := s.prepare(now); !wake.Equal(now.Add(refillGrace)) {
		t.Fatalf("stale reader needs a bounded wake at grace expiry; got %s", wake)
	}
	if lane, _, ok := q.next(20, now.Add(refillGrace)); !ok || lane != bulkLane {
		t.Fatal("reader failure continued to reserve its turn after grace")
	}
	q.enqueue(batch{route: route{0, singleLane}, tasks: []task{{}}, more: false})
	q.dispatched(singleLane, 20, now)
	if lane, _, ok := q.next(20, now); !ok || lane != bulkLane {
		t.Fatal("idle reader did not immediately donate unused capacity")
	}
}

func TestDeadlineAgingOnlyWithinChosenPriority(t *testing.T) {
	now := time.Unix(2000, 0)
	s := scheduler{batchPercent: 1}
	low := &s.queues[2]
	low.enqueue(batch{route: route{2, bulkLane}, tasks: []task{{admitBy: now.Add(admissionBudget - ageThreshold)}}})
	low.enqueue(batch{route: route{2, singleLane}, tasks: []task{{admitBy: now.Add(admissionBudget)}}})
	s.queues[0].enqueue(batch{route: route{0, singleLane}, tasks: []task{{admitBy: now.Add(admissionBudget)}}})
	r, _, _, ok := s.next(now)
	if !ok || r != (route{0, singleLane}) {
		t.Fatalf("aging stole a high priority turn: %+v", r)
	}
	s.cursor = len(weighted) - 1
	r, _, credit, ok := s.next(now)
	if !ok || r != (route{2, bulkLane}) {
		t.Fatalf("aging did not service old 1%% batch: %+v", r)
	}
	low.lanes[bulkLane] = append(low.lanes[bulkLane], low.lanes[bulkLane][0])
	low.dispatched(bulkLane, credit, now)
	if lane, _, ok := low.next(1, now.Add(time.Millisecond)); !ok || lane != singleLane {
		t.Fatal("one overdue bulk poll took multiple consecutive turns")
	}
}

func TestExpiredHeadReplaysWithoutSpendingTurn(t *testing.T) {
	now := time.Unix(2000, 0)
	s := scheduler{batchPercent: 20, quota: openQuota{}}
	q := &s.queues[0]
	results := make(chan error, 1)
	q.enqueue(batch{route: route{0, bulkLane}, tasks: []task{{admitBy: now, result: results}, {admitBy: now.Add(time.Minute)}}})
	q.enqueue(batch{route: route{0, singleLane}, tasks: []task{{admitBy: now.Add(time.Minute)}}})
	q.batchCredit = 80
	s.prepare(now)
	if len(results) != 1 || (<-results) != context.DeadlineExceeded {
		t.Fatal("expired task must return for Kafka replay")
	}
	r, _, _, ok := s.next(now)
	if !ok || r != (route{0, bulkLane}) || len(q.lanes[bulkLane]) != 1 {
		t.Fatal("expired task consumed the next bulk turn")
	}
}

func TestChannelReadersSplitEachLaneFetchBudget(t *testing.T) {
	for p, priority := range priorities {
		for _, lane := range []ingressLane{bulkLane, singleLane} {
			laneName, share := "batch", 20
			if lane == singleLane {
				laneName, share = "single", 80
			}
			for _, workers := range []int{1, 7, 24, 48} {
				var total int
				for i, channel := range channels {
					r := reader{route{p, lane}, i + 1}
					if got := r.suffix(); got != priority+"."+laneName+"."+channel {
						t.Fatalf("wrong channel topic suffix %q", got)
					}
					window := r.window(workers, 20)
					if window != newWindow(window.cap, share) {
						t.Fatalf("wrong starting window for %s: %+v", r.suffix(), window)
					}
					total += window.cap
				}
				if want := max(pollLimit(workers, p), len(channels)); total != want {
					t.Fatalf("%s with %d workers: aggregate cap %d, want %d", priority, workers, total, want)
				}
			}
		}
	}
}

func TestChannelReadersKeepIndependentRefillSignals(t *testing.T) {
	var q priorityQueue
	now := time.Now()
	q.enqueue(batch{route: route{1, bulkLane}, source: 1, tasks: []task{{}}, more: true})
	q.enqueue(batch{route: route{1, bulkLane}, source: 2, tasks: []task{{}}})
	q.dispatched(bulkLane, 0, now)
	q.dispatched(bulkLane, 0, now)
	q.enqueue(batch{route: route{1, bulkLane}, source: 3})
	if !q.refilling(bulkLane, now) {
		t.Fatal("an idle channel cleared the backlogged SMS reader's refill grace")
	}
	q.enqueue(batch{route: route{1, bulkLane}, source: 1, tasks: []task{{}}})
	if q.refilling(bulkLane, now) {
		t.Fatal("SMS refill did not clear its own grace")
	}
}
