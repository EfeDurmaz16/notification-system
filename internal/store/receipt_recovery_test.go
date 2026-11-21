package store

import (
	"errors"
	"testing"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/delivery"
)

func TestOnlyMatchingAuthoritativeReceiptRepairsExpiredLease(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	out := delivery.Outcome{State: "accepted", MessageID: "provider-receipt", Reason: "provider_accepted", HTTPStatus: 202}
	t.Run("unrelated unknown reason", func(t *testing.T) {
		id := createOne(t, s, sms())
		if ok, err := s.Start(ctx, id, 1); err != nil || !ok {
			t.Fatalf("claim: %v, %v", ok, err)
		}
		if err := s.Finish(ctx, id, 1, delivery.Outcome{State: "unknown", Reason: "transport_outcome_unknown"}); err != nil {
			t.Fatal(err)
		}
		if err := s.Finish(ctx, id, 1, out); !errors.Is(err, ErrConflict) {
			t.Fatalf("replaced another unknown outcome: %v", err)
		}
		n, err := s.Get(ctx, id)
		if err != nil || n.Status != "unknown" || n.LastReason != "transport_outcome_unknown" || n.MessageID != "" {
			t.Fatalf("unexpected outcome after conflicting receipt: %+v, %v", n, err)
		}
	})
	for _, tc := range []struct {
		name   string
		window time.Duration
	}{
		{"unknown", 0},
		{"retry_wait", time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := createOne(t, s, sms())
			if ok, err := s.Start(ctx, id, 1); err != nil || !ok {
				t.Fatalf("claim: %v, %v", ok, err)
			}
			if _, err := s.db.ExecContext(ctx, "UPDATE notifications SET updated_at=now()-interval '3 minutes' WHERE id=$1", id); err != nil {
				t.Fatal(err)
			}
			if err := s.TickWithRecovery(ctx, time.Minute, tc.window, 3); err != nil {
				t.Fatal(err)
			}
			if n, err := s.Get(ctx, id); err != nil || n.Status != tc.name {
				t.Fatalf("before receipt: %+v, %v", n, err)
			}
			if err := s.Finish(ctx, id, 2, out); !errors.Is(err, ErrConflict) {
				t.Fatalf("wrong generation: %v", err)
			}
			for _, invalid := range []delivery.Outcome{
				{State: "unknown", Reason: "malformed_ack", HTTPStatus: 202},
				{State: "accepted", Reason: out.Reason, HTTPStatus: 202},
				{State: "accepted", MessageID: out.MessageID, Reason: out.Reason, HTTPStatus: 500},
			} {
				if err := s.Finish(ctx, id, 1, invalid); !errors.Is(err, ErrConflict) {
					t.Fatalf("invalid receipt %+v: %v", invalid, err)
				}
			}
			for range 2 {
				if err := s.Finish(ctx, id, 1, out); err != nil {
					t.Fatal(err)
				}
			}
			if n, err := s.Get(ctx, id); err != nil || n.Status != "accepted" || n.NextAttemptAt != nil || n.MessageID != out.MessageID {
				t.Fatalf("recovered receipt: %+v, %v", n, err)
			}
		})
	}
}
