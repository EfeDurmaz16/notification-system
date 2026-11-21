package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/delivery"
	"github.com/EfeDurmaz16/notification-system/internal/store"
	"github.com/redis/go-redis/v9"
)

// The worker only uses Redis for receipts after a failed PostgreSQL write.
// Members contain no recipient, message content or provider credentials.
type receiptStore struct {
	delivery.Store
	client *redis.Client
	key    string
}

type receipt struct {
	ID        string
	Attempt   int
	MessageID string
	Status    int
}

func (s receiptStore) Finish(ctx context.Context, id string, attempt int, out delivery.Outcome) error {
	err := s.Store.Finish(ctx, id, attempt, out)
	if err == nil || out.State != "accepted" || out.Reason != "provider_accepted" || out.MessageID == "" {
		return err
	}
	// The database retry budget can already be exhausted. Acknowledge the
	// independent write before returning to the Kafka consumer.
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	r := receipt{ID: id, Attempt: attempt, MessageID: out.MessageID, Status: out.HTTPStatus}
	data, marshalErr := json.Marshal(r)
	if marshalErr != nil {
		return errors.Join(err, marshalErr)
	}
	if saveErr := s.client.SAdd(saveCtx, s.key, data).Err(); saveErr != nil {
		return errors.Join(err, fmt.Errorf("buffer provider receipt: %w", saveErr))
	}
	slog.WarnContext(ctx, "provider receipt buffered for database recovery", "notification_id", id, "attempt", attempt)
	// The database is still unconfirmed. Replaying the ready event cannot
	// reclaim this generation, and reconciliation never dispatches a POST.
	return err
}

func (s receiptStore) recover(ctx context.Context) error {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		if err := s.reconcile(ctx); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "provider receipt recovery deferred", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

func (s receiptStore) reconcile(ctx context.Context) error {
	// Sample without removing. A replacement repeats the same guarded write
	// if this worker dies before acknowledging the member.
	members, err := s.client.SRandMemberN(ctx, s.key, 32).Result()
	if err != nil {
		return err
	}
	return s.reconcileMembers(ctx, members)
}

func (s receiptStore) reconcileMembers(ctx context.Context, members []string) error {
	var deferred error
	consecutiveFailures := 0
	for _, data := range members {
		if err := ctx.Err(); err != nil {
			return err
		}
		var r receipt
		err := json.Unmarshal([]byte(data), &r)
		if err != nil || !validID.MatchString(r.ID) || r.Attempt < 1 || strings.TrimSpace(r.MessageID) == "" || strings.ContainsRune(r.MessageID, 0) || r.Status < 200 || r.Status > 299 {
			err = store.ErrConflict
		} else {
			out := delivery.Outcome{State: "accepted", MessageID: r.MessageID, HTTPStatus: r.Status, Reason: "provider_accepted"}
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = s.Store.Finish(writeCtx, r.ID, r.Attempt, out)
			cancel()
		}
		switch {
		case errors.Is(err, store.ErrConflict):
			// Preserve conflicting or superseded receipts for inspection.
			moved, moveErr := s.client.SMove(ctx, s.key, s.key+":conflicts", data).Result()
			if moveErr != nil {
				return moveErr
			}
			if moved {
				slog.WarnContext(ctx, "provider receipt requires inspection", "notification_id", r.ID, "attempt", r.Attempt)
			}
		case err != nil:
			// Leave this receipt buffered and try its neighbour. Two failed
			// database writes in a row stop this sample during an outage.
			deferred = err
			consecutiveFailures++
			if consecutiveFailures >= 2 {
				return err
			}
			continue
		default:
			if err := s.client.SRem(ctx, s.key, data).Err(); err != nil {
				return err
			}
		}
		consecutiveFailures = 0
	}
	return deferred
}
