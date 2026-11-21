package delivery

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"testing"
)

func TestAcceptanceLogPreservesCorrelationAndReceipt(t *testing.T) {
	for _, tc := range []struct {
		name      string
		finishErr error
		level     string
	}{
		{name: "saved", level: "INFO"},
		{name: "persistence uncertain", finishErr: errors.New("connection closed during finish"), level: "ERROR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var log bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&log, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })
			j := initialJob()
			j.CorrelationID = "checkout-42"
			e := Engine{Store: &recordStore{finishErr: tc.finishErr}, URL: "http://provider.invalid", Client: &http.Client{Transport: acceptedTransport{}}}
			if err := e.Send(t.Context(), j); !errors.Is(err, tc.finishErr) {
				t.Fatalf("send: %v", err)
			}
			var entry map[string]any
			if err := json.Unmarshal(log.Bytes(), &entry); err != nil {
				t.Fatal(err)
			}
			if entry["correlation_id"] != j.CorrelationID || entry["notification_id"] != j.ID || entry["status"] != accepted || entry["provider_http_status"] != float64(202) || entry["provider_message_id"] != "m1" || entry["level"] != tc.level || entry["persistence_confirmed"] != (tc.finishErr == nil) {
				t.Fatalf("incomplete provider result log: %v", entry)
			}
		})
	}
}
