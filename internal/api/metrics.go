package api

import (
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/EfeDurmaz16/notification-system/internal/store"
)

func writeOutcomeMetrics(w io.Writer, m store.Metrics) {
	// Terminal records are retained. Consumers can derive rates over the
	// desired interval with rate(notification_outcomes_total[5m]).
	fmt.Fprintln(w, "# TYPE notification_outcomes_total counter")
	for _, status := range []string{"accepted", "failed", "unknown", "cancelled"} {
		fmt.Fprintf(w, "notification_outcomes_total{status=%q} %d\n", status, m.Counts[status])
	}
	fmt.Fprintln(w, "# HELP notification_outcomes_per_second Terminal notifications per second averaged over the trailing five minutes.")
	fmt.Fprintln(w, "# TYPE notification_outcomes_per_second gauge")
	for _, channel := range []string{"sms", "email", "push"} {
		for _, status := range []string{"accepted", "failed", "unknown"} {
			count := m.FiveMinuteOutcomes[store.ChannelOutcome{Channel: channel, Status: status}]
			fmt.Fprintf(w, "notification_outcomes_per_second{channel=%q,status=%q,window=\"5m\"} %g\n", channel, status, float64(count)/300)
		}
	}
	fmt.Fprintln(w, "# TYPE notification_acceptance_latency_seconds histogram")
	for _, upper := range slices.Sorted(maps.Keys(m.LatencyBuckets)) {
		fmt.Fprintf(w, "notification_acceptance_latency_seconds_bucket{le=\"%g\"} %d\n", upper, m.LatencyBuckets[upper])
	}
	fmt.Fprintf(w, "notification_acceptance_latency_seconds_bucket{le=\"+Inf\"} %d\n", m.LatencyCount)
	fmt.Fprintln(w, "notification_acceptance_latency_seconds_sum", m.LatencySumSeconds)
	fmt.Fprintln(w, "notification_acceptance_latency_seconds_count", m.LatencyCount)
}
