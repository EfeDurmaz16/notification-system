package store

import "context"

// Snapshots reads the status of every distinct notification watched on one API replica.
func (s *Store) Snapshots(ctx context.Context, ids []string) (map[string]Notification, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+columns+" FROM notifications WHERE id=ANY($1::text[]::uuid[])", ids)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	items := make(map[string]Notification, len(ids))
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		items[n.ID] = n
	}
	return items, rows.Err()
}
