package store

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/delivery"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestLostDatabaseAckAfterCommittedReceipt(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	id := createOne(t, s, sms())
	ok, err := s.Start(ctx, id, 1)
	if !ok || err != nil {
		t.Fatalf("claim %v: %v", ok, err)
	}
	out := delivery.Outcome{State: "accepted", Reason: "provider_accepted", MessageID: "only-receipt", HTTPStatus: 202}
	var calls int
	var firstAt time.Time
	err = retryOutcomeWrite(ctx, func(ctx context.Context) error {
		calls++
		if err := s.finishOnce(ctx, id, 1, out); err != nil {
			return err
		}
		if calls == 1 {
			n, err := s.Get(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			firstAt = n.UpdatedAt
			return io.ErrUnexpectedEOF // The ACK for the committed write was lost.
		}
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("writes=%d err=%v", calls, err)
	}
	n, err := s.Get(ctx, id)
	if err != nil || n.Status != "accepted" || n.MessageID != out.MessageID || !n.UpdatedAt.Equal(firstAt) {
		t.Fatalf("replayed receipt: %+v %v", n, err)
	}
	var events int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM status_events WHERE notification_id=$1 AND status='accepted'`, id).Scan(&events); err != nil || events != 1 {
		t.Fatalf("receipt history: %d %v", events, err)
	}
	for _, changed := range []delivery.Outcome{
		{State: "unknown", Reason: "transport_outcome_unknown"},
		{State: "accepted", Reason: out.Reason, MessageID: "different", HTTPStatus: 202},
		{State: "accepted", Reason: "malformed_ack", MessageID: out.MessageID, HTTPStatus: 202},
		{State: "accepted", Reason: out.Reason, MessageID: out.MessageID, HTTPStatus: 200},
	} {
		if err := s.Finish(ctx, id, 1, changed); !errors.Is(err, ErrConflict) {
			t.Fatalf("conflicting result %+v returned %v", changed, err)
		}
	}
}

func TestReceiptRetryStopsOnPermanentErrorOrBudget(t *testing.T) {
	for _, tc := range []struct {
		err   error
		calls int
	}{
		{ErrConflict, 1},
		{&pgconn.PgError{Code: "23514"}, 1},
		{&pgconn.PgError{Code: "40001"}, 3},
		{io.EOF, 3},
	} {
		calls := 0
		err := retryOutcomeWrite(t.Context(), func(context.Context) error { calls++; return tc.err })
		if calls != tc.calls || !errors.Is(err, tc.err) {
			t.Fatalf("err=%v attempts=%d, want %v/%d", err, calls, tc.err, tc.calls)
		}
	}
}

func TestReceiptReplayRequiresSameRetryDeadlineAndGeneration(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	id := createOne(t, s, sms())
	if claimed, err := s.Start(ctx, id, 1); err != nil || !claimed {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	out := delivery.Outcome{State: "retry_wait", Reason: "dial_failed", NextAttemptAt: time.Now().Add(-time.Minute)}
	if err := s.Finish(ctx, id, 1, out); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(ctx, id, 1, out); err != nil {
		t.Fatalf("same deadline: %v", err)
	}
	changed := out
	changed.NextAttemptAt = changed.NextAttemptAt.Add(time.Millisecond)
	if err := s.Finish(ctx, id, 1, changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed deadline: %v", err)
	}
	if err := s.Tick(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(ctx, id, 1, out); !errors.Is(err, ErrConflict) {
		t.Fatalf("superseded generation: %v", err)
	}
}
