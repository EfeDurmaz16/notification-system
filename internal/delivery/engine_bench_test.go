package delivery

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
)

type acceptedTransport struct{}

func (acceptedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	_ = r.Body.Close()
	const response = `{"messageId":"m1","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`
	return &http.Response{StatusCode: http.StatusAccepted, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response))}, nil
}

// BenchmarkSendInProcess excludes TCP, PostgreSQL, Kafka and Redis. It exercises
// request and trace setup, the HTTP client, ack validation and a memory store.
func BenchmarkSendInProcess(b *testing.B) {
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	b.Cleanup(func() { slog.SetDefault(old) })
	j := initialJob()
	e := Engine{URL: "http://localhost/provider", Client: &http.Client{Transport: acceptedTransport{}}, Store: new(recordStore)}
	b.ReportAllocs()
	for b.Loop() {
		if err := e.Send(b.Context(), j); err != nil {
			b.Fatal(err)
		}
	}
}
