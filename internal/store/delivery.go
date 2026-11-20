package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/delivery"
)

func (s *Store) GetJob(ctx context.Context, id string) (delivery.Job, error) {
	var j delivery.Job
	var first sql.NullTime
	err := s.db.QueryRowContext(ctx, `SELECT id,recipient,channel,content,priority,attempt,traceparent,first_attempt_at
        FROM notifications WHERE id=$1`, id).Scan(&j.ID, &j.Recipient, &j.Channel, &j.Content, &j.Priority, &j.Attempt, &j.Traceparent, &first)
	if errors.Is(err, sql.ErrNoRows) {
		return j, ErrNotFound
	}
	j.FirstAttemptAt = first.Time
	return j, err
}
func (s *Store) Cancel(ctx context.Context, id string) error {
	// Cancellation is idempotent. Pending and future attempts have not acquired
	// send rights. PostgreSQL rechecks the predicate after concurrent updates.
	changed, err := s.db.ExecContext(ctx, `UPDATE notifications SET status='cancelled',next_attempt_at=NULL,updated_at=clock_timestamp()
        WHERE id=$1 AND status IN ('pending','scheduled') AND first_attempt_at IS NULL`, id)
	if err != nil {
		return err
	}
	n, err := changed.RowsAffected()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	var state string
	err = s.db.QueryRowContext(ctx, "SELECT status FROM notifications WHERE id=$1", id).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if state == "cancelled" {
		return nil
	}
	return ErrConflict
}
func (s *Store) Start(ctx context.Context, id string, attempt int) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE notifications SET status='sending',updated_at=clock_timestamp(),
        first_attempt_at=COALESCE(first_attempt_at,clock_timestamp())
        WHERE id=$1 AND attempt=$2 AND status='pending'`, id, attempt)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}
func (s *Store) Finish(ctx context.Context, id string, attempt int, state, messageID string, next time.Time) error {
	var deadline any
	switch state {
	case "retry_wait":
		if next.IsZero() {
			return errors.New("retry requires a deadline")
		}
		deadline = next
	case "accepted", "failed", "unknown":
	default:
		return fmt.Errorf("invalid finish state %q", state)
	}
	r, err := s.db.ExecContext(ctx, `UPDATE notifications SET status=$3,message_id=$4,next_attempt_at=$5,updated_at=clock_timestamp()
        WHERE id=$1 AND attempt=$2 AND status='sending'`, id, attempt, state, messageID, deadline)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrConflict
	}
	return nil
}

// Tick promotes due jobs in bounded chunks. Retry generation and publication
// commit together, so older Kafka records cannot acquire a newer attempt.
func (s *Store) Tick(ctx context.Context, staleAfter time.Duration) error {
	return s.TickWithRecovery(ctx, staleAfter, 0, 1)
}

// TickWithRecovery requeues an abandoned send only under an active provider
// idempotency contract. The worker checks the window again before posting.
func (s *Store) TickWithRecovery(ctx context.Context, staleAfter, idempotencyWindow time.Duration, maxAttempts int) error {
	if staleAfter <= 0 {
		return errors.New("positive sending timeout required")
	}
	_, err := s.db.ExecContext(ctx, `WITH due AS (
        SELECT id FROM notifications WHERE status IN ('retry_wait','scheduled') AND next_attempt_at<=now()
        ORDER BY next_attempt_at,id LIMIT 1000 FOR UPDATE SKIP LOCKED
    ),added AS (
        UPDATE notifications n SET status='pending',updated_at=clock_timestamp(),next_attempt_at=NULL,
            attempt=n.attempt+CASE WHEN n.status='retry_wait' THEN 1 ELSE 0 END
        FROM due WHERE n.id=due.id RETURNING n.id,n.priority,n.ingress,n.status,n.attempt,n.traceparent
    ) `+emitReady)
	if err != nil {
		return err
	}
	// A non-idempotent provider may already have applied the abandoned POST.
	_, err = s.db.ExecContext(ctx, `WITH expired AS (
        SELECT id, (attempt<$3 AND first_attempt_at + ($2::bigint * interval '1 millisecond') > now()) AS safe
        FROM notifications WHERE status='sending' AND updated_at < now()-($1::bigint * interval '1 millisecond')
        ORDER BY updated_at LIMIT 1000 FOR UPDATE SKIP LOCKED
    ) UPDATE notifications n SET status=CASE WHEN expired.safe THEN 'retry_wait' ELSE 'unknown' END,
        next_attempt_at=CASE WHEN expired.safe THEN now() END,updated_at=clock_timestamp()
        FROM expired WHERE n.id=expired.id`, staleAfter.Milliseconds(), idempotencyWindow.Milliseconds(), maxAttempts)
	return err
}
