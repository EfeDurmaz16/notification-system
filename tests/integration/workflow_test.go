// Package integration exercises the running Compose stack through its public HTTP surfaces.
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type createResult struct {
	ID      string
	BatchID string
}
type batchItem struct {
	ID, Error string
	Index     int
}
type batchResult struct {
	BatchID string
	Items   []batchItem
}
type notification struct {
	ID, Status, Content, MessageID, LastReason string
	ProviderHTTPStatus                         *int
}

func TestWorkflow(t *testing.T) {
	if os.Getenv("RUN_SMOKE") != "1" {
		t.Skip("make smoke runs this test against a live local stack")
	}
	api := strings.TrimRight(os.Getenv("API_URL"), "/")
	provider := strings.TrimRight(os.Getenv("MOCK_PROVIDER_URL"), "/")
	if api == "" || provider == "" {
		t.Fatal("API_URL and MOCK_PROVIDER_URL are required")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	key := fmt.Sprintf("smoke-%d", time.Now().UnixNano())
	t.Log("checking API and synthetic provider readiness")
	call(t, client, "GET", api+"/healthz", "", "", 200, nil)
	call(t, client, "GET", provider+"/health", "", "", 200, nil)

	// Cancellation wins while the item is scheduled, including a reconnectable
	// WebSocket observation. Wait past its original due time before counting POSTs.
	due := time.Now().UTC().Add(8 * time.Second)
	body := fmt.Sprintf(`{"recipient":"+12025550111","channel":"sms","priority":"normal","template":"Hi {{name}}","variables":{"name":"Ada"},"scheduledAt":%q}`, due.Format(time.RFC3339Nano))
	var created, replayed createResult
	call(t, client, "POST", api+"/v1/notifications", key, body, 202, &created)
	call(t, client, "POST", api+"/v1/notifications", key, body, 202, &replayed)
	if created.ID == "" || created.BatchID == "" || created != replayed {
		t.Fatalf("idempotent replay: %+v, %+v", created, replayed)
	}
	call(t, client, "POST", api+"/v1/notifications", key, strings.Replace(body, "Ada", "Grace", 1), 409, nil)
	itemURL := api + "/v1/notifications/" + created.ID
	wsURL := "ws" + strings.TrimPrefix(itemURL, "http") + "/events"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var state notification
	if err := conn.ReadJSON(&state); err != nil {
		t.Fatal(err)
	}
	if state.Status != "scheduled" || state.Content != "Hi Ada" {
		t.Fatalf("unexpected initial snapshot: %+v", state)
	}
	call(t, client, "POST", itemURL+"/cancel", "", "", 204, nil)
	if err := conn.ReadJSON(&state); err != nil {
		t.Fatal(err)
	}
	if state.Status != "cancelled" {
		t.Fatalf("cancellation update: %+v", state)
	}
	t.Log("verified scheduled template, idempotent replay, conflict and WebSocket cancellation")

	// A valid batch item traverses PostgreSQL, CDC, Kafka and the provider.
	batch := `{"notifications":[
		{"recipient":"+12025550111","channel":"sms","priority":"high","content":"Integration check"},
		{"recipient":"bad","channel":"sms","priority":"low","content":"invalid recipient"},
		{"recipient":"+12025550111","channel":"sms","priority":"low","content":"invalid year","scheduledAt":"0000-01-01T00:00:00Z"}
	]}`
	var b, repeat batchResult
	call(t, client, "POST", api+"/v1/batches", key+"-partial", batch, 202, &b)
	call(t, client, "POST", api+"/v1/batches", key+"-partial", batch, 202, &repeat)
	if !reflect.DeepEqual(b, repeat) || len(b.Items) != 3 || b.BatchID == "" || b.Items[0].ID == "" || b.Items[0].Index != 0 {
		t.Fatalf("partial batch replay: %+v, %+v", b, repeat)
	}
	for i := 1; i < len(b.Items); i++ {
		if b.Items[i].Index != i || b.Items[i].Error == "" || b.Items[i].ID != "" {
			t.Fatalf("invalid batch item %d: %+v", i, b.Items[i])
		}
	}
	received := awaitStatus(t, client, api+"/v1/notifications/"+b.Items[0].ID, "accepted")
	if received.MessageID == "" || received.LastReason != "provider_accepted" || received.ProviderHTTPStatus == nil || *received.ProviderHTTPStatus != 202 {
		t.Fatalf("missing provider acceptance evidence: %+v", received)
	}
	var fetched batchResult
	call(t, client, "GET", api+"/v1/batches/"+b.BatchID, "", "", 200, &fetched)
	if len(fetched.Items) != len(b.Items) || fetched.Items[0].ID != received.ID {
		t.Fatalf("batch query dropped accepted item: %+v", fetched)
	}
	t.Log("verified partial batch, replay and end-to-end provider acceptance")
	if wait := time.Until(due.Add(time.Second)); wait > 0 {
		time.Sleep(wait)
	}
	var attempts struct{ Attempts *int }
	call(t, client, "GET", provider+"/stats/"+created.ID, "", "", 200, &attempts)
	if attempts.Attempts == nil || *attempts.Attempts != 0 {
		t.Fatalf("cancelled item had a provider attempt or stats field was missing: %v", attempts.Attempts)
	}
	attempts.Attempts = nil
	call(t, client, "GET", provider+"/stats/"+received.ID, "", "", 200, &attempts)
	if attempts.Attempts == nil || *attempts.Attempts != 1 {
		t.Fatal("replayed batch should produce exactly one provider POST")
	}
	t.Log("verified zero actual POSTs for cancelled ID and exactly one for replayed accepted ID")

	// A definitive rejection and a lost acknowledgement are distinguishable
	// through the same public status resource used by an on-call engineer.
	for _, tc := range []struct {
		content, status, reason string
		httpStatus              int
	}{
		{"[mock:reject]", "failed", "provider_rejected", 400},
		{"[mock:unknown]", "unknown", "transport_outcome_unknown", 0},
	} {
		body := fmt.Sprintf(`{"recipient":"+12025550111","channel":"sms","priority":"normal","content":%q}`, tc.content)
		var item createResult
		call(t, client, "POST", api+"/v1/notifications", key+"-"+tc.status, body, 202, &item)
		outcome := awaitStatus(t, client, api+"/v1/notifications/"+item.ID, tc.status)
		if outcome.LastReason != tc.reason || (tc.httpStatus == 0) != (outcome.ProviderHTTPStatus == nil) || (outcome.ProviderHTTPStatus != nil && *outcome.ProviderHTTPStatus != tc.httpStatus) {
			t.Fatalf("provider evidence: %+v", outcome)
		}
	}

	for _, priority := range []string{"low", "normal", "high"} {
		t.Run("single_"+priority, func(t *testing.T) {
			body := fmt.Sprintf(`{"recipient":"+12025550111","channel":"sms","priority":%q,"content":"Standalone integration check"}`, priority)
			var single createResult
			call(t, client, "POST", api+"/v1/notifications", key+"-"+priority, body, 202, &single)
			state := awaitStatus(t, client, api+"/v1/notifications/"+single.ID, "accepted")
			var stats struct{ Attempts int }
			call(t, client, "GET", provider+"/stats/"+single.ID, "", "", 200, &stats)
			if state.MessageID == "" || stats.Attempts != 1 {
				t.Fatalf("single %s: messageId=%q provider attempts=%d, want 1", priority, state.MessageID, stats.Attempts)
			}
		})
	}
}

func awaitStatus(t *testing.T, client *http.Client, url, want string) notification {
	t.Helper()
	until := time.Now().Add(time.Minute)
	var n notification
	for time.Now().Before(until) {
		call(t, client, "GET", url, "", "", 200, &n)
		if n.Status == want {
			return n
		}
		if n.Status == "unknown" || n.Status == "failed" || n.Status == "cancelled" {
			t.Fatalf("unexpected terminal outcome: %+v", n)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s, last snapshot: %+v", want, n)
	return notification{}
}

func call(t *testing.T, client *http.Client, method, url, key, body string, want int, dest any) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != want {
		t.Fatalf("%s %s returned %d, want %d: %s", method, url, resp.StatusCode, want, bytes.TrimSpace(raw))
	}
	if dest != nil {
		if err := json.Unmarshal(raw, dest); err != nil {
			t.Fatal(err)
		}
	}
}
