package api

import (
	"context"
	"errors"
	"testing"

	"github.com/EfeDurmaz16/notification-system/internal/store"
)

func (s *repositoryStub) History(context.Context, string, int64) (store.HistoryPage, error) {
	return store.HistoryPage{Items: []store.StatusChange{}}, s.err
}

type historyStub struct {
	repositoryStub
	id    string
	after int64
}

func (s *historyStub) History(ctx context.Context, id string, after int64) (store.HistoryPage, error) {
	s.id, s.after = id, after
	return s.repositoryStub.History(ctx, id, after)
}

func TestHistoryRequests(t *testing.T) {
	path := "/v1/notifications/" + validID + "/history"
	for _, tc := range []struct {
		query  string
		err    error
		status int
		after  int64
	}{
		{"", nil, 200, 0}, {"?after=123", nil, 200, 123},
		{"?after=1&after=2", nil, 400, 0}, {"?offset=3", nil, 400, 0},
		{"?after=-1", nil, 400, 0}, {"?after=9223372036854775808", nil, 400, 0},
		{"", store.ErrNotFound, 404, 0}, {"", errors.New("store unavailable"), 503, 0},
	} {
		stub := &historyStub{repositoryStub: repositoryStub{err: tc.err}}
		w := call(New(stub), "GET", path+tc.query, "", "")
		if w.Code != tc.status || stub.after != tc.after {
			t.Fatalf("%s: status=%d after=%d", tc.query, w.Code, stub.after)
		}
		if tc.status != 400 && stub.id != validID || tc.status == 400 && stub.id != "" {
			t.Fatalf("%s: history called with ID %q", tc.query, stub.id)
		}
	}
}
