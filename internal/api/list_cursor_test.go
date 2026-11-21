package api

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/store"
)

type listStub struct {
	repositoryStub
	rows []store.Notification
}

func (s *listStub) List(_ context.Context, f store.Filter) ([]store.Notification, error) {
	s.filter = f
	return s.rows, nil
}

func TestListContinuation(t *testing.T) {
	at := time.Date(2026, 11, 21, 10, 0, 0, 123456000, time.UTC)
	repo := &listStub{rows: []store.Notification{{ID: validID, CreatedAt: at}, {ID: "00000000-0000-0000-0000-000000000001", CreatedAt: at}}}
	h := New(repo)
	path := "/v1/notifications?channel=sms&limit=1"
	r := call(h, "GET", path, "", "")
	var page struct {
		Items      []store.Notification
		NextCursor string
	}
	if err := json.Unmarshal(r.Body.Bytes(), &page); err != nil || r.Code != 200 || len(page.Items) != 1 || page.NextCursor == "" || repo.filter.Limit != 2 {
		t.Fatalf("first page: %d %s", r.Code, r.Body.String())
	}
	repo.rows = repo.rows[1:]
	next := call(h, "GET", path+"&cursor="+page.NextCursor, "", "")
	before := repo.filter.Before
	if next.Code != 200 || before == nil || before.ID != validID || !before.CreatedAt.Equal(at) || repo.filter.Channel != "sms" {
		t.Fatalf("continuation: %d %s, filter %+v", next.Code, next.Body.String(), repo.filter)
	}
	var end map[string]any
	if err := json.Unmarshal(next.Body.Bytes(), &end); err != nil || end["nextCursor"] != nil {
		t.Fatalf("last page: %s", next.Body.String())
	}
	for _, query := range []string{"&cursor=", "&cursor=bad", "&cursor=" + page.NextCursor + "&offset=0"} {
		if r := call(h, "GET", path+query, "", ""); r.Code != 400 {
			t.Errorf("expected invalid continuation for %q, got %d", query, r.Code)
		}
	}
}
