package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/store"
	"github.com/gorilla/websocket"
)

func TestSubscriptionCapacityAndReconnect(t *testing.T) {
	repo := &repositoryStub{item: store.Notification{ID: validID, Status: "pending"}}
	handler := limitSubscriptions(1, server{repo}.events)
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
