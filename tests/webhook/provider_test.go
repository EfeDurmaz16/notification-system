package webhook_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/delivery"
)

// Keep the external smoke opt-in. The production delivery client exercises
// the provider boundary; local integration tests cover persistence and Kafka.
func TestWebhookSiteCapture(t *testing.T) {
	if os.Getenv("RUN_WEBHOOK_SITE_SMOKE") != "1" {
		t.Skip("opt in with scripts/webhook-smoke.sh")
	}
	client := &http.Client{
		Timeout: 12 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	const base = "https://webhook.site"
	const messageID = "00000000-0000-4000-8000-000000000001"
	providerReply := map[string]string{
		"messageId": messageID, "status": "accepted",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	reply, err := json.Marshal(providerReply)
	if err != nil {
		t.Fatal(err)
	}
	var token struct{ UUID string }
	call(t, client, "configure", http.MethodPost, base+"/token", map[string]any{
		"default_status": 202, "default_content": string(reply), "default_content_type": "application/json",
		"expiry": 600, "request_limit": 5, "actions": false,
	}, &token)
	if !regexp.MustCompile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$").MatchString(token.UUID) {
		t.Fatal("invalid capture identifier")
	}
	// Token expires automatically when cleanup is unavailable on a free account.
	t.Cleanup(func() {
		req, err := http.NewRequest(http.MethodDelete, base+"/token/"+token.UUID, nil)
		if err != nil {
			t.Fatal("could not construct cleanup request")
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Log("token cleanup unavailable; configured TTL 600 seconds")
			return
		}
		_ = resp.Body.Close()
		t.Logf("disposable token cleanup status=%d", resp.StatusCode)
	})
	job := delivery.Job{
		ID: "00000000-0000-4000-8000-000000000002", Attempt: 1,
		Recipient: "assessment-smoke@example.invalid", Channel: "email", Priority: "normal",
		Content: "Synthetic notification integration smoke.",
	}
	s := &observedStore{}
	e := delivery.Engine{Store: s, Client: client, URL: base + "/" + token.UUID}
	if err := e.Send(t.Context(), job); err != nil {
		// Errors from HTTP clients may include the secret capture URL.
		t.Fatal("delivery returned an error; endpoint redacted")
	}
	if s.claims != 1 || s.finishes != 1 || s.state != "accepted" || s.messageID != messageID {
		t.Fatalf("provider outcome claims=%d finishes=%d state=%q messageIdMatch=%t", s.claims, s.finishes, s.state, s.messageID == messageID)
	}
	var history struct {
		Total int
		Data  []struct{ Method, Content string }
	}
	for range 5 {
		call(t, client, "history", http.MethodGet, base+"/token/"+token.UUID+"/requests", nil, &history)
		if history.Total > 0 {
			break
		}
		time.Sleep(time.Second)
	}
	if history.Total != 1 || len(history.Data) != 1 || history.Data[0].Method != http.MethodPost {
		t.Fatalf("expected one captured POST; count=%d", history.Total)
	}
	var body map[string]string
	if err := json.Unmarshal([]byte(history.Data[0].Content), &body); err != nil {
		t.Fatal("capture is not a JSON string map")
	}
	if !maps.Equal(body, map[string]string{"to": job.Recipient, "channel": job.Channel, "content": job.Content}) {
		t.Fatal("captured provider payload differs from expected to/channel/content")
	}
	t.Logf("observed=%s provider_status=202 outcome=accepted captured_posts=1 message_id_verified=true payload_verified=true",
		time.Now().UTC().Format(time.RFC3339))
}

type observedStore struct {
	claims, finishes int
	state, messageID string
}

func (s *observedStore) Start(context.Context, string, int) (bool, error) {
	s.claims++
	return s.claims == 1, nil
}

func (s *observedStore) Finish(_ context.Context, _ string, _ int, state, messageID string, _ time.Time) error {
	s.finishes++
	s.state, s.messageID = state, messageID
	return nil
}

func call(t *testing.T, c *http.Client, phase, method, url string, payload, dest any) {
	t.Helper()
	var body io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			t.Fatal("could not marshal " + phase)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, url, body)
	if err != nil {
		t.Fatal("could not build request for " + phase)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s: provider connection failed; endpoint redacted", phase)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("%s: HTTP %d", phase, resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(dest); err != nil {
		t.Fatalf("%s: invalid provider JSON", phase)
	}
}
