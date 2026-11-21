package stream

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBrokerConnectionFailureLoggedOnceAcrossReaders(t *testing.T) {
	var buf bytes.Buffer
	log := &brokerLog{logger: slog.New(slog.NewJSONHandler(&buf, nil))}
	for range 2 {
		c, err := kgo.NewClient(kgo.SeedBrokers("example.invalid:9092"), kgo.WithHooks(log),
			kgo.Dialer(func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("private dial detail") }),
			kgo.RequestRetries(0))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		err = c.Ping(ctx)
		cancel()
		c.Close()
		if err == nil {
			t.Fatal("expected a failed Kafka dial")
		}
	}
	out := buf.String()
	if strings.Count(out, "Kafka broker connection failed") != 1 || strings.Contains(out, "private dial detail") || strings.Contains(out, "example.invalid") {
		t.Fatalf("expected one sanitized broker warning, got %q", out)
	}
}
