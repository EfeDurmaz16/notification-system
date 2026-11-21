package stream

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	"github.com/EfeDurmaz16/notification-system/internal/delivery"
	"github.com/EfeDurmaz16/notification-system/internal/store"
	"github.com/redis/go-redis/v9"
)

type interruptedReceiptStore struct {
	claimed bool
	outcome delivery.Outcome
	err     error
}

func (s *interruptedReceiptStore) Start(context.Context, string, int) (bool, error) {
	if s.claimed {
		return false, nil
	}
	s.claimed = true
	return true, nil
}

func (s *interruptedReceiptStore) Finish(_ context.Context, _ string, _ int, out delivery.Outcome) error {
	if s.err == nil {
		s.outcome = out
	}
	return s.err
}

func TestReplacementRecoversReceiptWithoutAnotherPost(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR for cross-worker receipt recovery")
	}
	ctx := t.Context()
	db := &interruptedReceiptStore{err: io.ErrUnexpectedEOF}
	key := "test:receipts:" + rand.Text()
	replica := func() receiptStore {
		client := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() { _ = client.Close() })
		return receiptStore{Store: db, key: key, client: client}
	}
	first, replacement := replica(), replica()
	defer func() { _ = first.client.Del(ctx, key, key+":conflicts").Err() }()
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		posts.Add(1)
		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprint(w, `{"status":"accepted","messageId":"one","timestamp":"2026-01-01T00:00:00Z"}`)
	}))
	defer server.Close()
	engine := delivery.Engine{Store: first, URL: server.URL}
	job := delivery.Job{ID: "18f53374-fe69-40cb-9fe7-7f059d7db7f6", Channel: "sms", Attempt: 1}
	if err := engine.Send(ctx, job); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("write error: %v", err)
	}
	if err := replacement.reconcile(ctx); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("while database is down: %v", err)
	}
	if n, err := first.client.SCard(ctx, key).Result(); n != 1 || err != nil {
		t.Fatalf("durable receipts %d: %v", n, err)
	}
	db.err = nil
	engine.Store = replacement
	if err := engine.Send(ctx, job); err != nil { // Kafka redelivers the event.
		t.Fatal(err)
	}
	if err := replacement.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := first.client.SCard(ctx, key).Result(); err != nil || n != 0 || posts.Load() != 1 || db.outcome.MessageID != "one" || db.outcome.State != "accepted" {
		t.Fatalf("backlog %d, posts %d, outcome %+v, err %v", n, posts.Load(), db.outcome, err)
	}
	db.err = store.ErrConflict
	if err := first.Finish(ctx, job.ID, job.Attempt, db.outcome); !errors.Is(err, store.ErrConflict) {
		t.Fatal(err)
	}
	if err := replacement.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := first.client.SCard(ctx, key+":conflicts").Result(); err != nil || n != 1 {
		t.Fatalf("superseded receipt %d: %v", n, err)
	}
}
