package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Dial failures can be retried inside franz-go without surfacing in PollRecords.
// All readers in a worker share this warning budget.
type brokerLog struct {
	mu          sync.Mutex
	nextWarning time.Time
	logger      *slog.Logger
}

func (b *brokerLog) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if now.Before(b.nextWarning) {
		return
	}
	b.nextWarning = now.Add(30 * time.Second)
	b.logger.Warn("Kafka broker connection failed", "broker_id", meta.NodeID, "error_type", fmt.Sprintf("%T", err))
}
