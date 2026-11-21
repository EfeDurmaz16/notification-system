package store

import (
	"slices"
	"testing"
	"time"
)

func TestListFiltersAndPagination(t *testing.T) {
	s := testStore(t)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	add := func(channel, status string, created time.Time) string {
		t.Helper()
		item := sms()
		item.Channel = channel
		id := createOne(t, s, item)
		_, err := s.db.ExecContext(t.Context(), "UPDATE notifications SET status=$1, created_at=$2 WHERE id=$3", status, created, id)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	add("sms", "pending", start.Add(-time.Second))
	oldest := add("sms", "pending", start)
	middleA := add("sms", "pending", start.Add(time.Hour))
	middleB := add("sms", "pending", start.Add(time.Hour))
	newest := add("sms", "pending", start.Add(2*time.Hour))
	add("sms", "accepted", start.Add(3*time.Hour))
	add("push", "pending", start.Add(4*time.Hour))
	add("sms", "pending", end)

	ties := []string{middleA, middleB}
	slices.Sort(ties)
	wantOrder := []string{newest, ties[1], ties[0], oldest}
	for _, tc := range []struct {
		name          string
		limit, offset int
		want          []string
	}{
		{"combined filters and inclusive start", 100, 0, wantOrder},
		{"page one", 2, 0, wantOrder[:2]},
		{"page two", 2, 2, wantOrder[2:]},
		{"after last", 2, 4, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := s.List(t.Context(), Filter{Status: "pending", Channel: "sms", From: &start, To: &end, Limit: tc.limit, Offset: tc.offset})
			if err != nil {
				t.Fatal(err)
			}
			var ids []string
			for _, n := range rows {
				ids = append(ids, n.ID)
			}
			if !slices.Equal(ids, tc.want) {
				t.Errorf("IDs in descending creation and ID order = %v, want %v", ids, tc.want)
			}
		})
	}
}
