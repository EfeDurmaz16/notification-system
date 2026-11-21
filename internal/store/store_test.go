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

	"github.com/EfeDurmaz16/notification-system/internal/delivery"
	"github.com/EfeDurmaz16/notification-system/internal/observability"
)

// Each test owns a unique schema. The supplied database and all other schemas
// remain untouched, including when other integration processes run concurrently.
func testDSN(t *testing.T) string {
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
	return u.String()
}

func testStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), testDSN(t), false)
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
	r, err := s.Create(t.Context(), uuid(), []byte("digest"), SingleIngress, []Item{item})
	if err != nil {
		t.Fatal(err)
	}
	return r.Items[0].ID
}

func TestFiveMinuteOutcomeCountsByChannel(t *testing.T) {
	s := testStore(t)
	// A terminal row represents one notification even if acceptance took several
	// attempts. Each timestamp is relative to the database clock.
	_, err := s.db.ExecContext(t.Context(), `WITH batch AS (
	    INSERT INTO batches (id,idempotency_key,request_hash,response)
	    VALUES (gen_random_uuid(),'rates','digest','{}') RETURNING id
	)
	INSERT INTO notifications
	    (id,batch_id,item_index,recipient,channel,content,priority,status,attempt,created_at,updated_at)
	SELECT gen_random_uuid(),batch.id,row_number() OVER (), 'recipient',channel,'content','normal',
	    status,attempt,statement_timestamp()-created_age,statement_timestamp()-outcome_age
	FROM batch CROSS JOIN (VALUES
	    ('sms',   'accepted', 3, interval '2 hours',   interval '290 seconds'),
	    ('sms',   'accepted', 1, interval '2 hours',   interval '310 seconds'),
	    ('sms',   'failed',   2, interval '1 hour',    interval '10 seconds'),
	    ('email', 'accepted', 1, interval '1 minute',  interval '0 seconds'),
	    ('email', 'accepted', 1, interval '1 minute',  interval '10 seconds'),
	    ('email', 'failed',   1, interval '1 minute',  interval '-1 hour'),
	    ('push',  'unknown',  2, interval '1 hour',    interval '30 seconds'),
	    ('push',  'cancelled',1, interval '1 minute',  interval '5 seconds'),
	    ('push',  'pending',  1, interval '2 hours',   interval '2 hours'),
	    ('email', 'pending',  1, interval '1 hour',    interval '1 hour'),
	    ('sms',   'retry_wait',1,interval '1 minute',  interval '10 seconds')
	) AS items(channel,status,attempt,created_age,outcome_age)`)
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := s.Metrics(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := map[ChannelOutcome]int64{
		{Channel: "sms", Status: "accepted"}:   1,
		{Channel: "sms", Status: "failed"}:     1,
		{Channel: "email", Status: "accepted"}: 2,
		{Channel: "push", Status: "unknown"}:   1,
	}
	if !reflect.DeepEqual(metrics.FiveMinuteOutcomes, want) {
		t.Errorf("five-minute outcomes = %v, want %v", metrics.FiveMinuteOutcomes, want)
	}
	if metrics.Counts["accepted"] != 4 || metrics.Counts["pending"] != 2 || metrics.OldestPendingSeconds < 7200 {
		t.Errorf("lifetime totals or oldest pending age lost across channels: %+v", metrics)
	}
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
		go func() {
			<-fire
			r, e := s.Create(ctx, "replay", []byte("same"), BatchIngress, items)
			results <- result{r, e}
		}()
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
	if _, err := s.Create(ctx, "replay", []byte("different"), BatchIngress, items); !errors.Is(err, ErrConflict) {
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
	replay, err := s.Create(ctx, "replay", []byte("same"), BatchIngress, items)
	if err != nil || !reflect.DeepEqual(replay, original) {
		t.Fatalf("replay after cancellation: %+v %v", replay, err)
	}
}
func TestReadyEventsKeepIngress(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	for _, priority := range []string{"low", "normal", "high"} {
		for _, ingress := range []Ingress{SingleIngress, BatchIngress} {
			t.Run(priority+"/"+string(ingress), func(t *testing.T) {
				item := sms()
				item.Priority = priority
				key := uuid()
				result, err := s.Create(ctx, key, []byte("digest"), ingress, []Item{item})
				if err != nil {
					t.Fatal(err)
				}
				replay, err := s.Create(ctx, key, []byte("digest"), ingress, []Item{item})
				if err != nil || !reflect.DeepEqual(result, replay) {
					t.Fatalf("replay = %+v, %v", replay, err)
				}
				id := result.Items[0].ID
				if ok, err := s.Start(ctx, id, 1); err != nil || !ok {
					t.Fatalf("claim = %v, %v", ok, err)
				}
				if err := s.Finish(ctx, id, 1, delivery.Outcome{State: "retry_wait", Reason: "dial_failed", NextAttemptAt: time.Now().Add(-time.Minute)}); err != nil {
					t.Fatal(err)
				}
				if err := s.Tick(ctx, time.Hour); err != nil {
					t.Fatal(err)
				}
				wantTopic := "notifications." + priority
				if ingress == SingleIngress {
					wantTopic += ".single"
				}
				var count, correct int
				err = s.db.QueryRow(`SELECT count(*), count(*) FILTER (WHERE aggregatetype=$2)
					FROM outbox WHERE aggregateid=$1`, id, wantTopic).Scan(&count, &correct)
				if err != nil || count != 2 || correct != 2 {
					t.Fatalf("events = %d; on %s = %d; error = %v", count, wantTopic, correct, err)
				}
			})
		}
	}
}

func TestLegacyScheduledNotificationUsesBatchTopic(t *testing.T) {
	ctx := t.Context()
	dsn := testDSN(t)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	v1, err := migrations.ReadFile("migrations/001_notification.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, string(v1)); err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());
		INSERT INTO schema_migrations(version) VALUES('migrations/001_notification.sql')`)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid()
	_, err = db.ExecContext(ctx, `WITH old_batch AS (
		INSERT INTO batches(id,idempotency_key,request_hash,response) VALUES(gen_random_uuid(),'old','hash','{}') RETURNING id)
		INSERT INTO notifications(id,batch_id,item_index,recipient,channel,content,priority,status,attempt,next_attempt_at)
		SELECT $1,id,0,'+15555550100','sms','legacy','low','scheduled',1,now() FROM old_batch`, id)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(ctx, dsn, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Tick(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	var ingress, topic string
	err = db.QueryRow(`SELECT n.ingress, o.aggregatetype FROM notifications n
		JOIN outbox o ON o.aggregateid=n.id WHERE n.id=$1`, id).Scan(&ingress, &topic)
	if err != nil || ingress != "batch" || topic != "notifications.low" {
		t.Fatalf("legacy ingress=%q topic=%q error=%v", ingress, topic, err)
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
	ctx := observability.WithCorrelationID(t.Context(), "original-create")
	item := sms()
	future := time.Now().Add(time.Hour)
	item.ScheduledAt = &future
	res, err := s.Create(ctx, uuid(), []byte("digest"), SingleIngress, []Item{item})
	if err != nil {
		t.Fatal(err)
	}
	id := res.Items[0].ID
	job, err := s.GetJob(ctx, id)
	if err != nil || job.CorrelationID != "original-create" {
		t.Fatalf("correlation after database round trip: %q, %v", job.CorrelationID, err)
	}
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
	if err := s.Finish(ctx, id, 1, delivery.Outcome{State: "retry_wait", Reason: "dial_failed", NextAttemptAt: time.Now().Add(-time.Second)}); err != nil {
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
	if err := s.Finish(ctx, id, 1, delivery.Outcome{State: "accepted", MessageID: "old", Reason: "provider_accepted", HTTPStatus: 202}); !errors.Is(err, ErrConflict) {
		t.Fatalf("late finish: %v", err)
	}
	if err := s.Finish(ctx, id, 2, delivery.Outcome{State: "accepted", MessageID: "remote", Reason: "provider_accepted", HTTPStatus: 202}); err != nil {
		t.Fatal(err)
	}
	// A subsequent successful attempt replaces the current outcome but must
	// preserve the earlier retry's evidence in its own status event.
	var recorded int
	if err := s.db.QueryRow(`SELECT count(*) FROM status_events WHERE notification_id=$1 AND
	    ((attempt=1 AND status='retry_wait' AND reason='dial_failed' AND provider_http_status IS NULL) OR
	     (attempt=2 AND status='accepted' AND reason='provider_accepted' AND provider_http_status=202))`, id).Scan(&recorded); err != nil || recorded != 2 {
		t.Fatalf("attempt history has %d matching results: %v", recorded, err)
	}
	if err := s.db.QueryRow("SELECT count(*) FROM outbox WHERE aggregateid=$1 AND aggregatetype='notifications.high.single'", id).Scan(&out); err != nil || out != 2 {
		t.Fatalf("scheduled outbox count %d %v", out, err)
	}
	m, err := s.Metrics(ctx)
	if err != nil || m.LatencyCount != 1 || m.Counts["accepted"] != 1 {
		t.Fatalf("metrics: %+v %v", m, err)
	}
	n, err := s.List(ctx, Filter{Channel: "sms", Status: "accepted", Limit: 1})
	if err != nil || len(n) != 1 || n[0].ID != id || n[0].LastReason != "provider_accepted" || n[0].ProviderHTTPStatus == nil || *n[0].ProviderHTTPStatus != 202 || n[0].NextAttemptAt != nil {
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
			if err != nil || n.Status != tc.want || n.LastReason != "send_lease_expired" || n.ProviderHTTPStatus != nil || (n.NextAttemptAt != nil) != (tc.want == "retry_wait") {
				t.Fatalf("recovered status: %+v %v", n, err)
			}
		})
	}
}
func TestAtomicRollbackOnStoreFailure(t *testing.T) {
	s := testStore(t)
	a := sms()
	b := a
	b.Channel = "invalid"
	if _, err := s.Create(t.Context(), "rollback", []byte{1}, BatchIngress, []Item{a, b}); err == nil {
		t.Fatal("expected SQL constraint failure")
	}
	n := 0
	if err := s.db.QueryRow("SELECT (SELECT count(*) FROM batches)+(SELECT count(*) FROM notifications)+(SELECT count(*) FROM outbox)").Scan(&n); err != nil || n != 0 {
		t.Fatalf("partial persistence: %d %v", n, err)
	}
}

func TestBatchChannelRoutesOnCreateAndPromotion(t *testing.T) {
	s := testStore(t)
	s.channelBatch = true
	ctx := t.Context()
	entries := []Item{sms(), sms(), sms()}
	entries[1].Channel, entries[2].Channel = "email", "push"
	later := time.Now().Add(time.Hour)
	entries[2].ScheduledAt = &later
	created, err := s.Create(ctx, uuid(), []byte("channels"), BatchIngress, entries)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE notifications SET next_attempt_at=now() WHERE id=$1", created.Items[2].ID); err != nil {
		t.Fatal(err)
	}
	if err = s.Tick(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	countEvents := func(id, topic string) int {
		t.Helper()
		var n int
		if err := s.db.QueryRow("SELECT count(*) FROM outbox WHERE aggregateid=$1 AND aggregatetype=$2", id, topic).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for i, item := range entries {
		if countEvents(created.Items[i].ID, "notifications.high.batch."+item.Channel) != 1 {
			t.Fatalf("wrong batch topic for %s", item.Channel)
		}
	}
	if id := createOne(t, s, sms()); countEvents(id, "notifications.high.single") != 1 {
		t.Fatal("standalone topic changed")
	}
	// Roll writers back after the first attempt. The original record stays on
	// its channel topic. The due generation is published once on the shared one.
	id := created.Items[0].ID
	if started, err := s.Start(ctx, id, 1); err != nil || !started {
		t.Fatalf("start: %t %v", started, err)
	}
	if err := s.Finish(ctx, id, 1, delivery.Outcome{State: "retry_wait", Reason: "dial_failed", NextAttemptAt: time.Now().Add(-time.Second)}); err != nil {
		t.Fatal(err)
	}
	s.channelBatch = false
	if err := s.Tick(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	if countEvents(id, "notifications.high.batch.sms") != 1 || countEvents(id, "notifications.high") != 1 {
		t.Fatal("retry did not preserve the old event and publish on the active route")
	}
}
