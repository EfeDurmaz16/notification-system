package store

import "testing"

func TestSnapshotsReturnsWatchedIDsOnce(t *testing.T) {
	s := testStore(t)
	first, second := createOne(t, s, sms()), createOne(t, s, sms())
	if err := s.Cancel(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	items, err := s.Snapshots(t.Context(), []string{second, first, first, uuid()})
	if err != nil || len(items) != 2 || items[first].Status != "cancelled" || items[second].Status != "pending" {
		t.Fatalf("snapshots: %+v, error: %v", items, err)
	}
}
