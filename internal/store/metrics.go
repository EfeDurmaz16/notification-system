package store

import "context"

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
	bounds := [...]float64{0.1, 0.5, 1, 5, 10, 60}
	m := Metrics{Counts: make(map[string]int64), FiveMinuteOutcomes: make(map[ChannelOutcome]int64), LatencyBuckets: make(map[float64]int64)}
	for _, upper := range bounds {
		m.LatencyBuckets[upper] = 0
	}
	// A single statement provides a consistent snapshot across all three metrics.
	rows, err := s.db.QueryContext(ctx, `SELECT '' AS channel,status,bucket,sum(quantity),sum(latency_seconds)
		FROM notification_metric_totals GROUP BY status,bucket HAVING sum(quantity)>0
		UNION ALL
		SELECT channel,status,0,count(*),0 FROM notifications
		WHERE status IN ('accepted','failed','unknown') AND updated_at > statement_timestamp()-interval '5 minutes'
		AND updated_at <= statement_timestamp() GROUP BY channel,status
		UNION ALL
		SELECT '', '', 0, 0, greatest(0,extract(epoch FROM statement_timestamp()-(
		    SELECT created_at FROM notifications WHERE status='pending' ORDER BY created_at LIMIT 1)))`)
	if err != nil {
		return m, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var outcome ChannelOutcome
		var count int64
		var bucket int
		var seconds float64
		if err := rows.Scan(&outcome.Channel, &outcome.Status, &bucket, &count, &seconds); err != nil {
			return m, err
		}
		switch {
		case outcome.Status == "":
			m.OldestPendingSeconds = seconds
		case outcome.Channel != "":
			m.FiveMinuteOutcomes[outcome] = count
		default:
			m.Counts[outcome.Status] += count
			if outcome.Status == "accepted" {
				m.LatencyCount += count
				m.LatencySumSeconds += seconds
				for i, upper := range bounds {
					if i+1 >= bucket {
						m.LatencyBuckets[upper] += count
					}
				}
			}
		}
	}
	return m, rows.Err()
}
