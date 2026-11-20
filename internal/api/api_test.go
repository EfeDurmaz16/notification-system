package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/store"
	"github.com/gorilla/websocket"
)

const (
	validID    = "fb62ed66-de55-497f-a494-e89d763f6715"
	validInput = `{"channel":"sms","recipient":"+905550000111","content":"hello","priority":"normal"}`
)

type repositoryStub struct {
	mu      sync.Mutex
	ingress store.Ingress
	items   []store.Item
	hash    []byte
	result  store.BatchResult
	item    store.Notification
	filter  store.Filter
	err     error
}

func (s *repositoryStub) Create(_ context.Context, _ string, hash []byte, ingress store.Ingress, items []store.Item) (store.BatchResult, error) {
	if s.err != nil {
		return store.BatchResult{}, s.err
	}
	if len(s.hash) != 0 {
		if !bytes.Equal(s.hash, hash) {
			return store.BatchResult{}, store.ErrConflict
		}
		return s.result, nil
	}
	s.hash, s.items, s.ingress = hash, items, ingress
	s.result = store.BatchResult{BatchID: validID}
	for i, v := range items {
		res := store.ItemResult{Index: i, Error: v.Error}
		if v.Error == "" {
			res.ID = validID
			res.Status = "pending"
		}
		s.result.Items = append(s.result.Items, res)
	}
	return s.result, nil
}
func (s *repositoryStub) Get(_ context.Context, _ string) (store.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.item, s.err
}
func (s *repositoryStub) GetBatch(context.Context, string) (store.BatchResult, error) {
	return s.result, s.err
}
func (s *repositoryStub) Cancel(context.Context, string) error { return s.err }
func (s *repositoryStub) List(_ context.Context, f store.Filter) ([]store.Notification, error) {
	s.filter = f
	return nil, s.err
}
func (s *repositoryStub) Metrics(context.Context) (store.Metrics, error) {
	return store.Metrics{Counts: map[string]int64{"pending": 2, "failed": 1}, LatencyBuckets: map[float64]int64{1: 1, .1: 0}, LatencyCount: 1, LatencySumSeconds: .5}, s.err
}
func (s *repositoryStub) Ping(context.Context) error { return s.err }

func call(h http.Handler, method, path, body, key string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	r.Header.Set("X-Correlation-ID", "test-request")
	h.ServeHTTP(w, r)
	return w
}
func TestPartialBatchAndReplay(t *testing.T) {
	s := new(repositoryStub)
	h := New(s)
	body := `{"notifications":[` + validInput + `,{"channel":"sms","recipient":"wrong"},17]}`
	w := call(h, "POST", "/v1/batches", body, "run-1")
	if w.Code != 202 {
		t.Fatalf("status %d, body %s", w.Code, w.Body.String())
	}
	if w.Header().Get("X-Correlation-ID") != "test-request" {
		t.Fatal("correlation ID missing")
	}
	var result store.BatchResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 3 || result.Items[0].ID == "" || result.Items[1].Error == "" || result.Items[2].Index != 2 || result.Items[2].Error == "" {
		t.Fatalf("receipt %#v", result)
	}
	reordered := `{ "notifications": [{"priority":"normal","recipient":"+905550000111","channel":"sms","content":"hello"},{"recipient":"wrong","channel":"sms"},17] }`
	replay := call(h, "POST", "/v1/batches", reordered, "run-1")
	if replay.Code != 202 || replay.Body.String() != w.Body.String() {
		t.Fatalf("replay: %d %s", replay.Code, replay.Body.String())
	}
	conflict := call(h, "POST", "/v1/batches", strings.Replace(body, "hello", "hello again", 1), "run-1")
	if conflict.Code != 409 {
		t.Errorf("changed payload = %d", conflict.Code)
	}
}

func TestIngressUsesCreateEndpoint(t *testing.T) {
	for _, tc := range []struct {
		path, body string
		want       store.Ingress
	}{
		{"/v1/notifications", validInput, store.SingleIngress},
		{"/v1/batches", `{"notifications":[` + validInput + `]}`, store.BatchIngress},
	} {
		t.Run(tc.path, func(t *testing.T) {
			s := new(repositoryStub)
			w := call(New(s), "POST", tc.path, tc.body, "ingress-test")
			if w.Code != 202 || s.ingress != tc.want {
				t.Fatalf("status=%d ingress=%q want %q", w.Code, s.ingress, tc.want)
			}
		})
	}
}

func TestInvalidBatchReceiptReservesKey(t *testing.T) {
	h := New(new(repositoryStub))
	invalid := "{\"notifications\":[{}]}"
	w := call(h, "POST", "/v1/batches", invalid, "same")
	if w.Code != 422 {
		t.Fatal(w.Code, w.Body.String())
	}
	var receipt store.BatchResult
	if err := json.Unmarshal(w.Body.Bytes(), &receipt); err != nil || receipt.BatchID == "" || len(receipt.Items) != 1 || receipt.Items[0].Error == "" {
		t.Fatal("invalid receipt", receipt, err)
	}
	replay := call(h, "POST", "/v1/batches", invalid, "same")
	if replay.Code != 422 || replay.Body.String() != w.Body.String() {
		t.Fatal("invalid batch replay changed")
	}
	fixed := call(h, "POST", "/v1/batches", `{"notifications":[`+validInput+`]}`, "same")
	if fixed.Code != 409 {
		t.Fatal("changed request must use a new key", fixed.Code)
	}
}

func TestTemplateAndSchedule(t *testing.T) {
	s := new(repositoryStub)
	b := `{"recipient":"+905550000111","priority":"high","channel":"sms","template":"{{first}}: {{txt}}","variables":{"first":"Ada","txt":"{{ignored}}"},"scheduledAt":"2034-03-03T10:00:00Z"}`
	w := call(New(s), "POST", "/v1/notifications", b, "single")
	if w.Code != 202 || len(s.items) != 1 {
		t.Fatalf("%d: %s", w.Code, w.Body.String())
	}
	item := s.items[0]
	if item.Content != "Ada: {{ignored}}" || item.ScheduledAt == nil || item.ScheduledAt.Year() != 2034 {
		t.Fatalf("item %#v", item)
	}
}

func TestValidationAndBounds(t *testing.T) {
	cases := []struct {
		name, path, body, key string
		want                  int
	}{
		{"missing key", "/v1/notifications", validInput, "", 400},
		{"invalid key", "/v1/notifications", validInput, "a b", 400},
		{"oversize key", "/v1/notifications", validInput, strings.Repeat("k", 129), 400},
		{"invalid json", "/v1/notifications", "{", "k", 400},
		{"second document", "/v1/notifications", validInput + " true", "k", 400},
		{"unknown field", "/v1/notifications", strings.Replace(validInput, `"content"`, `"body"`, 1), "k", 422},
		{"malformed schedule", "/v1/notifications", strings.Replace(validInput, `"content"`, `"scheduledAt":"hello","content"`, 1), "k", 422},
		{"too many SMS characters", "/v1/notifications", strings.Replace(validInput, "hello", strings.Repeat("ü", 161), 1), "k", 422},
		{"unicode SMS", "/v1/notifications", strings.Replace(validInput, "hello", strings.Repeat("ü", 160), 1), "k", 202},
		{"no null bytes", "/v1/notifications", strings.Replace(validInput, "hello", `a\u0000b`, 1), "k", 422},
		{"oversize body", "/v1/notifications", strings.Repeat("a", maxBody+1), "k", 413},
		{"empty batch", "/v1/batches", `{"notifications":[]}`, "k", 400},
		{"batch limit", "/v1/batches", `{"notifications":[` + strings.Repeat("1,", 1000) + "1]}", "k", 400},
		{"all invalid", "/v1/batches", `{"notifications":[{},null]}`, "k", 422},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := call(New(new(repositoryStub)), "POST", c.path, c.body, c.key)
			if w.Code != c.want {
				t.Errorf("got %d want %d body %s", w.Code, c.want, w.Body.String())
			}
		})
	}
}
func TestTemplateErrors(t *testing.T) {
	cases := []struct {
		tmpl string
		vars map[string]string
	}{
		{"{{x}}", nil}, {"{{x", nil}, {"hello }}", nil}, {"{{.Method}}", nil}, {"{{x}}{{x}}", map[string]string{"x": strings.Repeat("s", 25)}},
	}
	for _, c := range cases {
		if _, err := render(c.tmpl, c.vars, 30); err == nil {
			t.Errorf("expected failure: %s", c.tmpl)
		}
	}
}

func TestFiltersAndMetrics(t *testing.T) {
	s := new(repositoryStub)
	h := New(s)
	w := call(h, "GET", "/v1/notifications?status=accepted&channel=sms&from=2027-01-01T00:00:00Z&limit=10&offset=2", "", "")
	if w.Code != 200 || s.filter.Status != "accepted" || s.filter.Offset != 2 || s.filter.From == nil || !strings.Contains(w.Body.String(), `"items":[]`) {
		t.Fatal(w.Code, w.Body.String(), s.filter)
	}
	for _, q := range []string{"limit=0", "limit=101", "offset=-1", "status=no", "channel=x", "to=t", "limit=1&limit=2"} {
		if w := call(h, "GET", "/v1/notifications?"+q, "", ""); w.Code != 400 {
			t.Errorf("%s: %d", q, w.Code)
		}
	}
	w = call(h, "GET", "/metrics", "", "")
	for _, want := range []string{"notification_queue_depth 2", `notification_outcomes_total{status="failed"} 1`, "notification_acceptance_latency_seconds_count 1"} {
		if !strings.Contains(w.Body.String(), want) {
			t.Errorf("metric missing: %q", want)
		}
	}
}
func TestCancelAndStorageErrors(t *testing.T) {
	s := new(repositoryStub)
	h := New(s)
	path := "/v1/notifications/" + validID + "/cancel"
	for _, tc := range []struct {
		err    error
		status int
	}{{nil, 204}, {store.ErrConflict, 409}, {store.ErrNotFound, 404}, {errors.New("unavailable"), 503}} {
		s.err = tc.err
		if w := call(h, "POST", path, "", ""); w.Code != tc.status {
			t.Errorf("%v: %d", tc.err, w.Code)
		}
	}
	if w := call(h, "GET", "/healthz", "", ""); w.Code != 503 {
		t.Fatal(w.Code)
	}
	if w := call(h, "GET", "/v1/notifications/bad", "", ""); w.Code != 404 {
		t.Fatalf("invalid ID got %d", w.Code)
	}
}

func TestWebSocketStatusSnapshots(t *testing.T) {
	s := &repositoryStub{item: store.Notification{ID: validID, Status: "pending", UpdatedAt: time.Now()}}
	srv := httptest.NewServer(New(s))
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/notifications/" + validID + "/events"
	c, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	var n store.Notification
	if err = c.ReadJSON(&n); err != nil || n.Status != "pending" {
		t.Fatal(n.Status, err)
	}
	s.mu.Lock()
	s.item.Status = "cancelled"
	s.item.UpdatedAt = time.Now()
	s.mu.Unlock()
	if err = c.ReadJSON(&n); err != nil || n.Status != "cancelled" {
		t.Fatal(n.Status, err)
	}
	if _, _, err = c.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
		t.Errorf("terminal close: %v", err)
	}
	_, resp, err = websocket.DefaultDialer.Dial(wsURL, http.Header{"Origin": []string{"https://another.test"}})
	if err == nil || resp == nil || resp.StatusCode != 403 {
		t.Fatal("expected cross-origin rejection", resp, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func BenchmarkValidateSMS(b *testing.B) {
	raw := []byte(validInput)
	b.ReportAllocs()
	for b.Loop() {
		if item := validateItem(raw); item.Error != "" {
			b.Fatal(item.Error)
		}
	}
}
