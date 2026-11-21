package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/delivery"
)

func (s *Store) GetJob(ctx context.Context, id string) (delivery.Job, error) {
	var j delivery.Job
	var first sql.NullTime
	err := s.db.QueryRowContext(ctx, `SELECT id,recipient,channel,content,priority,status,attempt,traceparent,correlation_id,first_attempt_at
        FROM notifications WHERE id=$1`, id).Scan(&j.ID, &j.Recipient, &j.Channel, &j.Content, &j.Priority, &j.Status, &j.Attempt, &j.Traceparent, &j.CorrelationID, &first)
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
func (s *Store) Finish(ctx context.Context, id string, attempt int, out delivery.Outcome) error {
	return retryOutcomeWrite(ctx, func(ctx context.Context) error { return s.finishOnce(ctx, id, attempt, out) })
}

func (s *Store) finishOnce(ctx context.Context, id string, attempt int, out delivery.Outcome) error {
	var deadline any
	switch out.State {
	case "retry_wait":
		if out.NextAttemptAt.IsZero() {
			return errors.New("retry requires a deadline")
		}
		deadline = out.NextAttemptAt
	case "accepted", "failed", "unknown":
		if !out.NextAttemptAt.IsZero() {
			return errors.New("terminal outcome cannot schedule a retry")
		}
	default:
		return fmt.Errorf("invalid finish state %q", out.State)
	}
	args := []any{id, attempt, out.State, out.MessageID, deadline, out.Reason, out.HTTPStatus}
	r, err := s.db.ExecContext(ctx, `UPDATE notifications SET status=$3,message_id=$4,next_attempt_at=$5,
        last_reason=$6,provider_http_status=NULLIF($7::integer,0),updated_at=clock_timestamp()
        WHERE id=$1 AND attempt=$2 AND status='sending'`, args...)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil || n == 1 {
		return err
	}
	// The database may have committed our exact outcome before the connection
	// dropped. Read back the evidence without updating timestamps or history.
	var matches bool
	err = s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM notifications
		WHERE id=$1 AND attempt=$2 AND status=$3 AND message_id=$4
		AND next_attempt_at IS NOT DISTINCT FROM $5::timestamptz AND last_reason=$6
		AND provider_http_status IS NOT DISTINCT FROM NULLIF($7::integer,0))`, args...).Scan(&matches)
	if err == nil && !matches {
		return ErrConflict
	}
	return err
}

// Keep three database attempts inside the worker's five-second receipt budget.
// The closure only persists an already observed outcome and cannot issue a POST.
func retryOutcomeWrite(ctx context.Context, write func(context.Context) error) error {
	for attempt := 1; ; attempt++ {
		writeCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		err := write(writeCtx)
		cancel()
		if err == nil || attempt == 3 || !transientWriteError(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
		}
	}
}

func transientWriteError(err error) bool {
	var sqlErr interface{ SQLState() string }
	if errors.As(err, &sqlErr) {
		switch sqlErr.SQLState() {
		case "08000", "08001", "08003", "08006", "08007", // connection failures
			"40001", "40P01", "55P03", // serialization, deadlock, lock timeout
			"57P01", "57P02", "57P03", "53300": // shutdown, restart, full pool
			return true
		default:
			return false
		}
	}
	var op *net.OpError
	return errors.As(err, &op) || errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded)
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
        FROM due WHERE n.id=due.id RETURNING n.id,n.priority,n.ingress,n.status,n.attempt,n.traceparent,n.channel,$1::boolean AS channel_batch
    ) `+emitReady, s.channelBatch)
	if err != nil {
		return err
	}
	// A non-idempotent provider may already have applied the abandoned POST.
	_, err = s.db.ExecContext(ctx, `WITH expired AS (
        SELECT id, (attempt<$3 AND first_attempt_at + ($2::bigint * interval '1 millisecond') > now()) AS safe
        FROM notifications WHERE status='sending' AND updated_at < now()-($1::bigint * interval '1 millisecond')
        ORDER BY updated_at LIMIT 1000 FOR UPDATE SKIP LOCKED
    ) UPDATE notifications n SET status=CASE WHEN expired.safe THEN 'retry_wait' ELSE 'unknown' END,
        next_attempt_at=CASE WHEN expired.safe THEN now() END,last_reason='send_lease_expired',
        provider_http_status=NULL,updated_at=clock_timestamp()
        FROM expired WHERE n.id=expired.id`, staleAfter.Milliseconds(), idempotencyWindow.Milliseconds(), maxAttempts)
	return err
}
