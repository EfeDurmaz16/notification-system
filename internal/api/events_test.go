package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/store"
	"github.com/gorilla/websocket"
)

func TestSubscriptionCapacityAndReconnect(t *testing.T) {
	repo := &repositoryStub{item: store.Notification{ID: validID, Status: "pending"}}
	handler := limitSubscriptions(1, newSnapshotFeed(repo).events)
	returned := make(chan struct{}, 3)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/notifications/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		handler(w, r)
		returned <- struct{}{}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/notifications/" + validID + "/events"
	first, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	_, rejected, err := websocket.DefaultDialer.Dial(url, nil)
	if err == nil || rejected == nil {
		t.Fatal("expected an HTTP response rejecting the second handshake")
	}
	_ = rejected.Body.Close()
	if rejected.StatusCode != http.StatusServiceUnavailable || rejected.Header.Get("Retry-After") != "1" {
		t.Fatalf("busy response: %d, headers %v", rejected.StatusCode, rejected.Header)
	}
	_ = first.Close()
	for range 2 {
		select {
		case <-returned:
		case <-time.After(3 * time.Second):
			t.Fatal("socket handler did not release its place after disconnect")
		}
	}
	repo.mu.Lock()
	repo.item.Status = "accepted"
	repo.mu.Unlock()
	reconnected, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reconnected.Close() }()
	_ = reconnected.SetReadDeadline(time.Now().Add(3 * time.Second))
	var snapshot store.Notification
	if err := reconnected.ReadJSON(&snapshot); err != nil || snapshot.Status != "accepted" {
		t.Fatalf("reconnected snapshot: %s, %v", snapshot.Status, err)
	}
}

func (s *repositoryStub) Snapshots(ctx context.Context, ids []string) (map[string]store.Notification, error) {
	n, err := s.Get(ctx, ids[0])
	return map[string]store.Notification{n.ID: n}, err
}

type recordingRepo struct {
	*repositoryStub
	queries chan []string
}

func (r *recordingRepo) Snapshots(ctx context.Context, ids []string) (map[string]store.Notification, error) {
	select {
	case r.queries <- slices.Clone(ids):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	rows := make(map[string]store.Notification, len(ids))
	for _, id := range ids {
		rows[id] = store.Notification{ID: id}
	}
	return rows, nil
}

func TestSnapshotFeedBatchesDistinctIDsAndReleasesIdlePoller(t *testing.T) {
	repo := &recordingRepo{repositoryStub: new(repositoryStub), queries: make(chan []string, 8)}
	f := newSnapshotFeed(repo)
	a, stopA := f.subscribe("a")
	slow, stopSlow := f.subscribe("a")
	b, stopB := f.subscribe("b")
	defer stopA()
	defer stopSlow()
	defer stopB()
	for range 3 {
		select {
		case ids := <-repo.queries:
			slices.Sort(ids)
			if !slices.Equal(ids, []string{"a", "b"}) {
				t.Fatalf("queried %v", ids)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("no batch query")
		}
		for id, ch := range map[string]<-chan snapshot{"a": a, "b": b} {
			select {
			case s := <-ch:
				if s.err != nil || s.notification.ID != id {
					t.Fatalf("wrong snapshot for %s: %+v", id, s)
				}
			case <-time.After(time.Second):
				t.Fatal("slow socket stalled fanout")
			}
		}
	}
	if s := <-slow; s.notification.ID != "a" {
		t.Fatalf("wrong snapshot on second subscription: %+v", s)
	}
	stopA()
	stopSlow()
	stopB()
	select {
	case <-repo.queries:
		t.Fatal("queried PostgreSQL after all subscriptions ended")
	case <-time.After(350 * time.Millisecond):
	}
}
