package store

import (
	"context"
)

type Metrics struct {
	Counts               map[string]int64
	OldestPendingSeconds float64
	LatencyBuckets       map[float64]int64
	LatencySumSeconds    float64
	LatencyCount         int64
}

func (s *Store) Metrics(ctx context.Context) (Metrics, error) {
	m := Metrics{Counts: map[string]int64{}, LatencyBuckets: map[float64]int64{}}
	rows, err := s.db.QueryContext(ctx, `SELECT status,count(*),GREATEST(0,EXTRACT(EPOCH FROM now()-min(created_at)))
        FROM notifications GROUP BY status`)
	if err != nil {
		return m, err
	}
	for rows.Next() {
		var status string
		var count int64
		var age float64
		if err = rows.Scan(&status, &count, &age); err != nil {
			rows.Close()
			return m, err
		}
		m.Counts[status] = count
		if status == "pending" {
			m.OldestPendingSeconds = age
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return m, err
	}
	rows.Close()
	var b [6]int64
	err = s.db.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(t),0), count(*) FILTER(WHERE t<=0.1),
        count(*) FILTER(WHERE t<=0.5),count(*) FILTER(WHERE t<=1),count(*) FILTER(WHERE t<=5),
        count(*) FILTER(WHERE t<=10),count(*) FILTER(WHERE t<=60)
        FROM (SELECT extract(epoch FROM updated_at-created_at) t FROM notifications WHERE status='accepted') q`).
		Scan(&m.LatencyCount, &m.LatencySumSeconds, &b[0], &b[1], &b[2], &b[3], &b[4], &b[5])
	for i, le := range []float64{0.1, 0.5, 1, 5, 10, 60} {
		m.LatencyBuckets[le] = b[i]
	}
	return m, err
}
