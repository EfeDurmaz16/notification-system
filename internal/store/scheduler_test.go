package store

import (
	"context"
	"testing"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/delivery"
)

func scheduledSMS(t *testing.T, s *Store) string {
	t.Helper()
	item := sms()
	future := time.Now().UTC().Add(time.Hour)
	item.ScheduledAt = &future
	return createOne(t, s, item)
}

func TestCancelledScheduledJobStaysCancelled(t *testing.T) {
	s := testStore(t)
	id := scheduledSMS(t, s)
	ctx := t.Context()

	if err := s.Tick(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	n, err := s.Get(ctx, id)
	if err != nil || n.Status != "scheduled" || n.NextAttemptAt == nil {
		t.Fatalf("future job after scheduler tick: %+v, %v", n, err)
	}
	for range 2 {
		if err := s.Cancel(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Tick(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	n, err = s.Get(ctx, id)
	if err != nil || n.Status != "cancelled" || n.NextAttemptAt != nil {
		t.Fatalf("after cancellation: %+v, %v", n, err)
	}
	started, err := s.Start(ctx, id, 1)
	if err != nil || started {
		t.Fatalf("late claim after cancellation: started=%t err=%v", started, err)
	}
	var cancelledEvents, readyEvents int
	err = s.db.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM status_events WHERE notification_id=$1 AND status='cancelled'),
		(SELECT count(*) FROM outbox WHERE aggregateid=$1)`, id).Scan(&cancelledEvents, &readyEvents)
	if err != nil || cancelledEvents != 1 || readyEvents != 0 {
		t.Fatalf("cancellation events=%d, ready events=%d, err=%v", cancelledEvents, readyEvents, err)
	}
}

func TestSchedulerSkipsLockedJobAndPublishesOnceWhenReleased(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	lockedID, availableID := scheduledSMS(t, s), scheduledSMS(t, s)
	if _, err := s.db.ExecContext(ctx, `UPDATE notifications SET next_attempt_at=now() WHERE id IN ($1,$2)`, lockedID, availableID); err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "SELECT id FROM notifications WHERE id=$1 FOR UPDATE", lockedID); err != nil {
		t.Fatal(err)
	}
	// A second scheduler must make progress while the first holds its row.
	tickCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := s.Tick(tickCtx, time.Minute); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{lockedID: "scheduled", availableID: "pending"} {
		n, err := s.Get(ctx, id)
		if err != nil || n.Status != want {
			t.Fatalf("job %s: got %s, want %s, err=%v", id, n.Status, want, err)
		}
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.Tick(ctx, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{lockedID, availableID} {
		var total, correct int
		err := s.db.QueryRowContext(ctx, `SELECT count(*), count(*) FILTER (
			WHERE type='NotificationReady' AND payload->>'id'=$1::uuid::text AND payload->>'attempt'='1'
		) FROM outbox WHERE aggregateid=$1`, id).Scan(&total, &correct)
		if err != nil || total != 1 || correct != 1 {
			t.Errorf("job %s: %d events, %d matching first attempts, err=%v", id, total, correct, err)
		}
	}
}

func TestRetryPromotionRollsBackIfOutboxWriteFails(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	id := createOne(t, s, sms())
	if claimed, err := s.Start(ctx, id, 1); err != nil || !claimed {
		t.Fatalf("claim: %t %v", claimed, err)
	}
	out := delivery.Outcome{State: "retry_wait", Reason: "dial_failed", NextAttemptAt: time.Now().Add(-time.Minute)}
	if err := s.Finish(ctx, id, 1, out); err != nil {
		t.Fatal(err)
	}
	// Reject the next publication only in this test's schema.
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE outbox ADD CONSTRAINT block_publish CHECK (false) NOT VALID`); err != nil {
		t.Fatal(err)
	}
	if err := s.Tick(ctx, time.Hour); err == nil {
		t.Fatal("scheduler reported success after failed publication")
	}
	job, err := s.Get(ctx, id)
	if err != nil || job.Status != "retry_wait" || job.Attempt != 1 || job.NextAttemptAt == nil {
		t.Fatalf("partial promotion: %+v, %v", job, err)
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE outbox DROP CONSTRAINT block_publish`); err != nil {
		t.Fatal(err)
	}
	if err := s.Tick(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	job, err = s.Get(ctx, id)
	if err != nil || job.Status != "pending" || job.Attempt != 2 || job.NextAttemptAt != nil {
		t.Fatalf("recovered promotion: %+v, %v", job, err)
	}
	var attempts string
	err = s.db.QueryRowContext(ctx, `SELECT string_agg(payload->>'attempt', ',' ORDER BY payload->>'attempt')
		FROM outbox WHERE aggregateid=$1`, id).Scan(&attempts)
	if err != nil || attempts != "1,2" {
		t.Fatalf("published attempts=%q, err=%v", attempts, err)
	}
}
