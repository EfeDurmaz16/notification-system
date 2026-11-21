package store

import (
	"context"
	"strconv"
	"time"
)

type StatusChange struct {
	Attempt            int       `json:"attempt"`
	Status             string    `json:"status"`
	ChangedAt          time.Time `json:"changedAt"`
	Reason             string    `json:"reason,omitempty"`
	ProviderHTTPStatus *int      `json:"providerHttpStatus,omitempty"`
}

type HistoryPage struct {
	Items     []StatusChange `json:"items"`
	NextAfter string         `json:"nextAfter,omitempty"`
}

// History reads one page from the index on (notification_id, seq).
func (s *Store) History(ctx context.Context, id string, after int64) (HistoryPage, error) {
	const pageSize = 50
	rows, err := s.db.QueryContext(ctx, `SELECT seq,attempt,status,changed_at,COALESCE(reason,''),provider_http_status
		FROM status_events WHERE notification_id=$1 AND seq>$2 ORDER BY seq LIMIT $3`, id, after, pageSize+1)
	if err != nil {
		return HistoryPage{}, err
	}
	defer func() { _ = rows.Close() }()
	page := HistoryPage{Items: []StatusChange{}}
	var last int64
	for rows.Next() {
		if len(page.Items) == pageSize {
			page.NextAfter = strconv.FormatInt(last, 10)
			break
		}
		var change StatusChange
		if err := rows.Scan(&last, &change.Attempt, &change.Status, &change.ChangedAt, &change.Reason, &change.ProviderHTTPStatus); err != nil {
			return HistoryPage{}, err
		}
		page.Items = append(page.Items, change)
	}
	if err := rows.Err(); err != nil {
		return HistoryPage{}, err
	}
	if len(page.Items) == 0 {
		// Retention and end-of-pagination both produce empty pages for a valid ID.
		var exists bool
		if err := s.db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM notifications WHERE id=$1)", id).Scan(&exists); err != nil {
			return HistoryPage{}, err
		}
		if !exists {
			return HistoryPage{}, ErrNotFound
		}
	}
	return page, nil
}
