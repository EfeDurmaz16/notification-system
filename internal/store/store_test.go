package store

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Each test owns a unique schema. The supplied database and all other schemas
// remain untouched, including when other integration processes run concurrently.
func testStore(t *testing.T) *Store {
	t.Helper()
	raw := os.Getenv("TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	schema := "test_" + strings.ReplaceAll(uuid(), "-", "")
	if _, err = db.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.Exec("DROP SCHEMA " + schema + " CASCADE"); err != nil {
			t.Error(err)
		}
	})
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	s, err := Open(context.Background(), u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func sms() Item {
	return Item{Recipient: "+15555550100", Channel: "sms", Priority: "high", Content: "test"}
}
func createOne(t *testing.T, s *Store, item Item) string {
	t.Helper()
	r, err := s.Create(t.Context(), uuid(), []byte("digest"), []Item{item})
	if err != nil {
		t.Fatal(err)
	}
	return r.Items[0].ID
}
func TestConcurrentCreateReplay(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	items := []Item{sms(), {Error: "recipient invalid"}, sms()}
	type result struct {
		r BatchResult
		e error
	}
	const count = 12
	results := make(chan result, count)
	fire := make(chan struct{})
	for range count {
		go func() { <-fire; r, e := s.Create(ctx, "replay", []byte("same"), items); results <- result{r, e} }()
	}
	close(fire)
	var original BatchResult
	for i := range count {
		got := <-results
		if got.e != nil {
			t.Fatal(got.e)
		}
		if i == 0 {
			original = got.r
		} else if !reflect.DeepEqual(original, got.r) {
			t.Fatalf("replay changed result: %+v vs %+v", original, got.r)
		}
	}
	if original.Items[1].Error == "" || original.Items[1].ID != "" {
		t.Fatal("invalid item accepted")
	}
	if _, err := s.Create(ctx, "replay", []byte("different"), items); !errors.Is(err, ErrConflict) {
		t.Fatalf("different payload: %v", err)
	}
	var notifications, outbox int
	if err := s.db.QueryRow("SELECT (SELECT count(*) FROM notifications),(SELECT count(*) FROM outbox)").Scan(&notifications, &outbox); err != nil {
		t.Fatal(err)
	}
	if notifications != 2 || outbox != 2 {
		t.Fatalf("notifications=%d outbox=%d", notifications, outbox)
	}
	id := original.Items[0].ID
	if err := s.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetBatch(ctx, original.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Items[0].Status != "cancelled" || current.Items[0].UpdatedAt == nil {
		t.Fatal(current)
	}
	replay, err := s.Create(ctx, "replay", []byte("same"), items)
	if err != nil || !reflect.DeepEqual(replay, original) {
		t.Fatalf("replay after cancellation: %+v %v", replay, err)
	}
}
func TestCancelSendRace(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	// Repeated independent races ensure both operations use a single atomic boundary.
	for range 24 {
		id := createOne(t, s, sms())
		start := make(chan struct{})
		cancelled := make(chan error, 1)
		type claimed struct {
			ok  bool
			err error
		}
		sent := make(chan claimed, 1)
		go func() { <-start; cancelled <- s.Cancel(ctx, id) }()
		go func() { <-start; ok, err := s.Start(ctx, id, 1); sent <- claimed{ok, err} }()
		close(start)
		c, cl := <-cancelled, <-sent
		if cl.err != nil {
			t.Fatal(cl.err)
		}
		if cl.ok {
			if !errors.Is(c, ErrConflict) {
				t.Fatalf("both succeeded: %v", c)
			}
		} else if c != nil {
			t.Fatalf("both lost: %v", c)
		}
		again, err := s.Start(ctx, id, 1)
		if err != nil || again {
			t.Fatalf("duplicate claim %v %v", again, err)
		}
	}
}
func TestGenerationFenceAndScheduledSend(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	item := sms()
	future := time.Now().Add(time.Hour)
	item.ScheduledAt = &future
	id := createOne(t, s, item)
	if ok, err := s.Start(ctx, id, 1); err != nil || ok {
		t.Fatalf("future claim: %v %v", ok, err)
	}
	var out int
	if err := s.db.QueryRow("SELECT count(*) FROM outbox").Scan(&out); err != nil || out != 0 {
		t.Fatalf("scheduled published early: %d %v", out, err)
	}
	if _, err := s.db.Exec("UPDATE notifications SET next_attempt_at=now() WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	if err := s.Tick(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Start(ctx, id, 1); err != nil || !ok {
		t.Fatalf("scheduled claim: %v %v", ok, err)
	}
	if err := s.Finish(ctx, id, 1, "retry_wait", "", time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.Tick(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Start(ctx, id, 1); err != nil || ok {
		t.Fatalf("stale attempt: %v %v", ok, err)
	}
	if err := s.Cancel(ctx, id); !errors.Is(err, ErrConflict) {
		t.Fatalf("cancel after external attempt: %v", err)
	}
	if ok, err := s.Start(ctx, id, 2); err != nil || !ok {
		t.Fatalf("retry claim: %v %v", ok, err)
	}
	if err := s.Finish(ctx, id, 1, "accepted", "old", time.Time{}); !errors.Is(err, ErrConflict) {
		t.Fatalf("late finish: %v", err)
	}
	if err := s.Finish(ctx, id, 2, "accepted", "remote", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow("SELECT count(*) FROM outbox WHERE aggregateid=$1", id).Scan(&out); err != nil || out != 2 {
		t.Fatalf("scheduled outbox count %d %v", out, err)
	}
	m, err := s.Metrics(ctx)
	if err != nil || m.LatencyCount != 1 || m.Counts["accepted"] != 1 {
		t.Fatalf("metrics: %+v %v", m, err)
	}
	n, err := s.List(ctx, Filter{Channel: "sms", Status: "accepted", Limit: 1})
	if err != nil || len(n) != 1 || n[0].ID != id {
		t.Fatalf("list: %+v %v", n, err)
	}
}
func TestStaleRecoveryHonorsProviderContract(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	for _, tc := range []struct {
		name   string
		window time.Duration
		max    int
		want   string
	}{
		{"no guarantee", 0, 3, "unknown"},
		{"active guarantee", time.Hour, 3, "retry_wait"},
		{"expired guarantee", time.Millisecond, 3, "unknown"},
		{"exhausted", time.Hour, 1, "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := createOne(t, s, sms())
			if ok, err := s.Start(ctx, id, 1); err != nil || !ok {
				t.Fatalf("claim: %v %v", ok, err)
			}
			_, err := s.db.Exec("UPDATE notifications SET updated_at=now()-interval '10 seconds',first_attempt_at=now()-interval '11 seconds' WHERE id=$1", id)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.TickWithRecovery(ctx, time.Second, tc.window, tc.max); err != nil {
				t.Fatal(err)
			}
			n, err := s.Get(ctx, id)
			if err != nil || n.Status != tc.want {
				t.Fatalf("got %s %v", n.Status, err)
			}
		})
	}
}
func TestAtomicRollbackOnStoreFailure(t *testing.T) {
	s := testStore(t)
	a := sms()
	b := a
	b.Channel = "invalid"
	if _, err := s.Create(t.Context(), "rollback", []byte{1}, []Item{a, b}); err == nil {
		t.Fatal("expected SQL constraint failure")
	}
	n := 0
	if err := s.db.QueryRow("SELECT (SELECT count(*) FROM batches)+(SELECT count(*) FROM notifications)+(SELECT count(*) FROM outbox)").Scan(&n); err != nil || n != 0 {
		t.Fatalf("partial persistence: %d %v", n, err)
	}
}
