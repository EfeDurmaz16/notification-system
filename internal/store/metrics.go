package store

import (
	"context"
)

type Metrics struct {
	Counts               map[string]int64
	FiveMinuteOutcomes   map[ChannelOutcome]int64
	OldestPendingSeconds float64
	LatencyBuckets       map[float64]int64
	LatencySumSeconds    float64
	LatencyCount         int64
}

type ChannelOutcome struct {
	Channel string
	Status  string
}

func (s *Store) Metrics(ctx context.Context) (Metrics, error) {
	m := Metrics{
		Counts:             make(map[string]int64),
		FiveMinuteOutcomes: make(map[ChannelOutcome]int64),
		LatencyBuckets:     make(map[float64]int64),
	}
	// Notifications are immutable after their terminal transition, so updated_at
	// is the recorded outcome time even when creation or earlier attempts are old.
	rows, err := s.db.QueryContext(ctx, `SELECT channel, status, count(*),
        GREATEST(0, EXTRACT(EPOCH FROM statement_timestamp() - min(created_at))),
        count(*) FILTER (WHERE status IN ('accepted','failed','unknown')
            AND updated_at > statement_timestamp() - interval '5 minutes'
            AND updated_at <= statement_timestamp())
        FROM notifications GROUP BY channel, status`)
	if err != nil {
		return m, err
	}
	for rows.Next() {
		var outcome ChannelOutcome
		var count, recent int64
		var age float64
		if err = rows.Scan(&outcome.Channel, &outcome.Status, &count, &age, &recent); err != nil {
			rows.Close()
			return m, err
		}
		m.Counts[outcome.Status] += count
		if recent > 0 {
			m.FiveMinuteOutcomes[outcome] = recent
		}
		if outcome.Status == "pending" {
			m.OldestPendingSeconds = max(m.OldestPendingSeconds, age)
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
