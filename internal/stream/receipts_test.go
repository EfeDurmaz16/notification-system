package stream

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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
	write   func(string) error
}

func (s *interruptedReceiptStore) Start(context.Context, string, int) (bool, error) {
	if s.claimed {
		return false, nil
	}
	s.claimed = true
	return true, nil
}

func (s *interruptedReceiptStore) Finish(_ context.Context, id string, _ int, out delivery.Outcome) error {
	if s.write != nil {
		return s.write(id)
	}
	if s.err == nil {
		s.outcome = out
	}
	return s.err
}

func TestReceiptStoreWhenRedisIsUnavailable(t *testing.T) {
	cacheDown := errors.New("receipt cache offline")
	dbDown := errors.New("no database connection")
	client := redis.NewClient(&redis.Options{
		MaxRetries: -1,
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			return nil, cacheDown
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	db := &interruptedReceiptStore{}
	s := receiptStore{Store: db, client: client, key: "unreachable"}
	out := delivery.Outcome{State: "accepted", Reason: "provider_accepted", MessageID: "confirmed", HTTPStatus: 202}
	ctx := t.Context()
	if err := s.Finish(ctx, "one", 1, out); err != nil || db.outcome != out {
		t.Fatalf("normal receipt depends on Redis: %+v, %v", db.outcome, err)
	}
	db.err = dbDown
	if err := s.Finish(ctx, "two", 1, out); !errors.Is(err, dbDown) || !errors.Is(err, cacheDown) {
		t.Fatalf("lost storage errors: %v", err)
	}
	if err := s.Finish(ctx, "three", 1, delivery.Outcome{State: "unknown"}); !errors.Is(err, dbDown) || errors.Is(err, cacheDown) {
		t.Fatalf("ambiguous response treated as a receipt: %v", err)
	}
	if err := s.reconcile(ctx); !errors.Is(err, cacheDown) {
		t.Fatalf("failed recovery scan was not reported: %v", err)
	}
}

func TestReceiptSampleProgressAndOutageBudget(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR for mixed receipt recovery")
	}
	ctx := t.Context()
	key := "test:receipt-errors:" + rand.Text()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	defer func() { _ = client.Del(ctx, key).Err() }()
	db := &interruptedReceiptStore{}
	s := receiptStore{Store: db, client: client, key: key}
	writeErr := errors.New("database write failed")
	// Supply the sampled order explicitly so the failing member is always first.
	var ids, members []string
	for i := range 4 {
		id := fmt.Sprintf("18f53374-fe69-40cb-9fe7-7f059d7db7f%d", i)
		data, err := json.Marshal(receipt{ID: id, Attempt: 1, Status: 202, MessageID: "confirmed"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		members = append(members, string(data))
	}
	for _, allDown := range []bool{false, true} {
		if err := client.SAdd(ctx, key, members).Err(); err != nil {
			t.Fatal(err)
		}
		writes := 0
		db.write = func(id string) error {
			writes++
			if allDown || id == ids[0] {
				return writeErr
			}
			return nil
		}
		if err := s.reconcileMembers(ctx, members); !errors.Is(err, writeErr) {
			t.Fatalf("failed receipt was not reported: %v", err)
		}
		count, err := client.SCard(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if allDown && (writes != 2 || count != 4) {
			t.Fatalf("database outage: writes=%d retained=%d", writes, count)
		}
		if !allDown && (writes != 4 || count != 1 || !client.SIsMember(ctx, key, members[0]).Val()) {
			t.Fatalf("isolated failure: writes=%d retained=%d", writes, count)
		}
	}
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
