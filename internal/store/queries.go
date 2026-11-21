package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Notification struct {
	LastReason         string     `json:"lastReason,omitempty"`
	ProviderHTTPStatus *int       `json:"providerHttpStatus,omitempty"`
	NextAttemptAt      *time.Time `json:"nextAttemptAt,omitempty"`
	ID                 string     `json:"id"`
	BatchID            string     `json:"batchId"`
	Recipient          string     `json:"recipient"`
	Channel            string     `json:"channel"`
	Content            string     `json:"content"`
	Priority           string     `json:"priority"`
	Status             string     `json:"status"`
	MessageID          string     `json:"messageId,omitempty"`
	Attempt            int        `json:"attempt"`
	CreatedAt          time.Time  `json:"createdAt"`
	UpdatedAt          time.Time  `json:"updatedAt"`
	ScheduledAt        *time.Time `json:"scheduledAt,omitempty"`
}
type Filter struct {
	Status, Channel, BatchID string
	From, To                 *time.Time
	Limit, Offset            int
	Before                   *ListPosition
}

// ListPosition identifies a row in the immutable creation-time ordering.
type ListPosition struct {
	CreatedAt time.Time
	ID        string
}

const columns = `id,batch_id,recipient,channel,content,priority,status,message_id,attempt,created_at,updated_at,scheduled_at,last_reason,provider_http_status,next_attempt_at`

type scanner interface{ Scan(...any) error }

func scanNotification(r scanner) (Notification, error) {
	var n Notification
	err := r.Scan(&n.ID, &n.BatchID, &n.Recipient, &n.Channel, &n.Content, &n.Priority, &n.Status, &n.MessageID, &n.Attempt, &n.CreatedAt, &n.UpdatedAt, &n.ScheduledAt, &n.LastReason, &n.ProviderHTTPStatus, &n.NextAttemptAt)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return n, err
}
func (s *Store) Get(ctx context.Context, id string) (Notification, error) {
	return scanNotification(s.db.QueryRowContext(ctx, "SELECT "+columns+" FROM notifications WHERE id=$1", id))
}
func (s *Store) List(ctx context.Context, f Filter) ([]Notification, error) {
	if f.Limit == 0 {
		f.Limit = 50
	}
	if f.Limit < 0 || f.Limit > 1000 || f.Offset < 0 {
		return nil, errors.New("invalid pagination")
	}
	var args []any
	var where []string
	add := func(col string, value any) {
		args = append(args, value)
		where = append(where, fmt.Sprintf("%s $%d", col, len(args)))
	}
	if f.Status != "" {
		add("status =", f.Status)
	}
	if f.Channel != "" {
		add("channel =", f.Channel)
	}
	if f.BatchID != "" {
		add("batch_id =", f.BatchID)
	}
	if f.From != nil {
		add("created_at >=", *f.From)
	}
	if f.To != nil {
		add("created_at <", *f.To)
	}
	if f.Before != nil {
		args = append(args, f.Before.CreatedAt, f.Before.ID)
		where = append(where, fmt.Sprintf("(created_at,id) < ($%d,$%d)", len(args)-1, len(args)))
	}
	q := "SELECT " + columns + " FROM notifications"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += fmt.Sprintf(" ORDER BY created_at DESC,id DESC LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2)
	args = append(args, f.Limit, f.Offset)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	items := []Notification{}
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, n)
	}
	return items, rows.Err()
}
func (s *Store) GetBatch(ctx context.Context, id string) (BatchResult, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, "SELECT response FROM batches WHERE id=$1", id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return BatchResult{}, ErrNotFound
	}
	if err != nil {
		return BatchResult{}, err
	}
	var result BatchResult
	if err = json.Unmarshal(raw, &result); err != nil {
		return BatchResult{}, err
	}
	rows, err := s.db.QueryContext(ctx, "SELECT item_index,status,created_at,updated_at FROM notifications WHERE batch_id=$1", id)
	if err != nil {
		return BatchResult{}, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var i int
		var status string
		var created, updated time.Time
		if err = rows.Scan(&i, &status, &created, &updated); err != nil {
			return BatchResult{}, err
		}
		if i < 0 || i >= len(result.Items) {
			return BatchResult{}, fmt.Errorf("invalid stored batch index %d", i)
		}
		result.Items[i].Status = status
		result.Items[i].CreatedAt = &created
		result.Items[i].UpdatedAt = &updated
	}
	return result, rows.Err()
}
