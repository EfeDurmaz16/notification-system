package store

import (
	"testing"
	"time"
)

func TestCreationCursorTraversesTiesAcrossNewInsert(t *testing.T) {
	s := testStore(t)
	for range 4 {
		createOne(t, s, sms())
	}
	stamp := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	if _, err := s.db.Exec("UPDATE notifications SET created_at=$1", stamp); err != nil {
		t.Fatal(err)
	}
	all, err := s.List(t.Context(), Filter{Channel: "sms"})
	if err != nil || len(all) != 4 {
		t.Fatalf("seed: %d rows, %v", len(all), err)
	}
	first, err := s.List(t.Context(), Filter{Limit: 2})
	if err != nil || len(first) != 2 {
		t.Fatalf("first page: %v %v", first, err)
	}
	createOne(t, s, sms())
	second, err := s.List(t.Context(), Filter{Limit: 2, Before: &ListPosition{CreatedAt: first[1].CreatedAt, ID: first[1].ID}})
	if err != nil || len(second) != 2 || second[0].ID != all[2].ID || second[1].ID != all[3].ID {
		t.Fatalf("second page: %v %v", second, err)
	}
}
