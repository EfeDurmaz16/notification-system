package store

import (
	"errors"
	"strconv"
	"testing"

	"github.com/EfeDurmaz16/notification-system/internal/delivery"
)

func TestNotificationHistory(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	id := createOne(t, s, sms())
	if ok, err := s.Start(ctx, id, 1); err != nil || !ok {
		t.Fatalf("claim: %t, %v", ok, err)
	}
	if err := s.Finish(ctx, id, 1, delivery.Outcome{State: "unknown", Reason: "malformed_ack", HTTPStatus: 200}); err != nil {
		t.Fatal(err)
	}
	other := createOne(t, s, sms())
	if err := s.Cancel(ctx, other); err != nil {
		t.Fatal(err)
	}
	page, err := s.History(ctx, id, 0)
	if err != nil || len(page.Items) != 3 || page.NextAfter != "" {
		t.Fatalf("history: %+v, %v", page, err)
	}
	for i, want := range []string{"pending", "sending", "unknown"} {
		change := page.Items[i]
		if change.Status != want || change.Attempt != 1 || change.ChangedAt.IsZero() {
			t.Fatalf("change %d: %+v", i, change)
		}
	}
	result := page.Items[2]
	if result.Reason != "malformed_ack" || result.ProviderHTTPStatus == nil || *result.ProviderHTTPStatus != 200 {
		t.Fatalf("result evidence: %+v", result)
	}

	if _, err := s.db.ExecContext(ctx, `INSERT INTO status_events(notification_id, attempt, status)
		SELECT $1, i, 'pending' FROM generate_series(2,51) i ORDER BY i`, id); err != nil {
		t.Fatal(err)
	}
	first, err := s.History(ctx, id, 0)
	if err != nil || len(first.Items) != 50 || first.Items[49].Attempt != 48 {
		t.Fatalf("first page: %+v, %v", first, err)
	}
	cursor, err := strconv.ParseInt(first.NextAfter, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	last, err := s.History(ctx, id, cursor)
	if err != nil || len(last.Items) != 3 || last.NextAfter != "" || last.Items[0].Attempt != 49 || last.Items[2].Attempt != 51 {
		t.Fatalf("last page: %+v, %v", last, err)
	}
	if _, err := s.db.ExecContext(ctx, "DELETE FROM status_events WHERE notification_id=$1", id); err != nil {
		t.Fatal(err)
	}
	empty, err := s.History(ctx, id, 0)
	if err != nil || empty.Items == nil || len(empty.Items) != 0 {
		t.Fatalf("expired history: %+v, %v", empty, err)
	}
	if _, err := s.History(ctx, uuid(), 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown ID: %v", err)
	}
}
