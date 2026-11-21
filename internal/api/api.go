// Package api serves the notification management HTTP API.
package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/EfeDurmaz16/notification-system/internal/observability"
	"github.com/EfeDurmaz16/notification-system/internal/store"
)

type Repository interface {
	Create(context.Context, string, []byte, store.Ingress, []store.Item) (store.BatchResult, error)
	Get(context.Context, string) (store.Notification, error)
	Snapshots(context.Context, []string) (map[string]store.Notification, error)
	History(context.Context, string, int64) (store.HistoryPage, error)
	GetBatch(context.Context, string) (store.BatchResult, error)
	Cancel(context.Context, string) error
	List(context.Context, store.Filter) ([]store.Notification, error)
	Metrics(context.Context) (store.Metrics, error)
	Ping(context.Context) error
}

type server struct{ repo Repository }

// New creates a handler without starting a listener or background goroutines.
func New(repo Repository) http.Handler {
	return NewWithSubscriptionLimit(repo, DefaultSubscriptionLimit)
}

const DefaultSubscriptionLimit = 32

// NewWithSubscriptionLimit bounds the number of open WebSockets per API process.
func NewWithSubscriptionLimit(repo Repository, limit int) http.Handler {
	s := server{repo}
	mux := http.NewServeMux()
	// Bound database work before the HTTP server's write timeout, including
	// when a client keeps its connection open. WebSockets have separate timers.
	handle := func(pattern string, handler http.HandlerFunc) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
			defer cancel()
			handler(w, r.WithContext(ctx))
		})
	}
	create := limitCreateBodies(s.create)
	handle("POST /v1/notifications", create)
	handle("POST /v1/batches", create)
	handle("GET /v1/notifications", s.list)
	handle("GET /v1/notifications/{id}", withID(s.get))
	handle("GET /v1/notifications/{id}/history", withID(s.history))
	handle("GET /v1/batches/{id}", withID(s.batch))
	handle("POST /v1/notifications/{id}/cancel", withID(s.cancel))
	mux.HandleFunc("GET /v1/notifications/{id}/events", withID(limitSubscriptions(limit, newSnapshotFeed(repo).events)))
	handle("GET /metrics", s.metrics)
	handle("GET /healthz", s.health)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		correlation := r.Header.Get("X-Correlation-ID")
		if !visibleKey(correlation) {
			var id [16]byte
			_, _ = rand.Read(id[:])
			correlation = hex.EncodeToString(id[:])
		}
		w.Header().Set("X-Correlation-ID", correlation)
		r = r.WithContext(observability.WithCorrelationID(r.Context(), correlation))
		response := &accessWriter{ResponseWriter: w, status: http.StatusOK}
		mux.ServeHTTP(response, r)
		level := slog.LevelInfo
		if r.Pattern == "GET /healthz" {
			level = slog.LevelWarn
			if response.status == http.StatusOK {
				level = slog.LevelDebug
			}
		}
		slog.Log(r.Context(), level, "http request", "correlation_id", correlation, "trace_id", observability.TraceID(r.Context()), "method", r.Method, "route", r.Pattern, "status", response.status, "duration_ms", time.Since(start).Milliseconds())
	})
}

var idPattern = regexp.MustCompile(`^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`)

func withID(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !idPattern.MatchString(r.PathValue("id")) {
			problem(w, http.StatusNotFound, "resource not found")
			return
		}
		next(w, r)
	}
}

func visibleKey(key string) bool {
	if len(key) < 1 || len(key) > 128 {
		return false
	}
	for i := range len(key) {
		if key[i] < 33 || key[i] > 126 {
			return false
		}
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func problem(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func repositoryError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		problem(w, 404, "resource not found")
	case errors.Is(err, store.ErrConflict):
		problem(w, 409, "request conflicts with existing state")
	default:
		slog.Error("repository operation failed", "correlation_id", w.Header().Get("X-Correlation-ID"), observability.StorageError(err))
		problem(w, 503, "storage temporarily unavailable")
	}
}

const maxBody = 5 << 20

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		mediaType, _, err := mime.ParseMediaType(ct)
		if err != nil || mediaType != "application/json" {
			problem(w, 415, "use application/json")
			return nil, false
		}
	}
	if r.ContentLength > maxBody {
		problem(w, 413, "body exceeds 5 MiB")
		return nil, false
	}
	var buf bytes.Buffer
	if r.ContentLength > 0 {
		// Leave room for ReadFrom to observe EOF without growing the buffer.
		buf.Grow(int(r.ContentLength) + bytes.MinRead)
	}
	_, err := buf.ReadFrom(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			problem(w, 413, "body exceeds 5 MiB")
		} else {
			problem(w, 400, "could not read request")
		}
		return nil, false
	}
	body := buf.Bytes()
	if !utf8.Valid(body) || !json.Valid(body) {
		problem(w, 400, "expected one UTF-8 JSON document")
		return nil, false
	}
	return body, true
}

func strictJSON(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func (s server) create(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if len(r.Header.Values("Idempotency-Key")) != 1 || !visibleKey(key) {
		problem(w, 400, "Idempotency-Key must contain 1 to 128 visible ASCII characters")
		return
	}
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	batch := r.URL.Path == "/v1/batches"
	ingress := store.SingleIngress
	if batch {
		ingress = store.BatchIngress
	}
	rawItems, hash, err := requestItems(r.URL.Path, body)
	if err != nil {
		problem(w, 400, err.Error())
		return
	}
	items := make([]store.Item, len(rawItems))
	for i, raw := range rawItems {
		items[i] = validateItem(raw)
	}
	res, err := s.repo.Create(r.Context(), key, hash, ingress, items)
	if err != nil {
		repositoryError(w, err)
		return
	}
	// The persisted receipt governs replay even after validation rules change.
	accepted := false
	for _, item := range res.Items {
		if item.ID != "" {
			accepted = true
			break
		}
	}
	if !accepted {
		if !batch {
			problem(w, 422, res.Items[0].Error)
			return
		}
		writeJSON(w, 422, res)
		return
	}
	if batch {
		writeJSON(w, 202, res)
		return
	}
	writeJSON(w, 202, map[string]string{"id": res.Items[0].ID, "batchId": res.BatchID})
}

func (s server) get(w http.ResponseWriter, r *http.Request) {
	n, err := s.repo.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		repositoryError(w, err)
		return
	}
	writeJSON(w, 200, n)
}

func (s server) batch(w http.ResponseWriter, r *http.Request) {
	b, err := s.repo.GetBatch(r.Context(), r.PathValue("id"))
	if err != nil {
		repositoryError(w, err)
		return
	}
	writeJSON(w, 200, b)
}

func (s server) cancel(w http.ResponseWriter, r *http.Request) {
	if err := s.repo.Cancel(r.Context(), r.PathValue("id")); err != nil {
		repositoryError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.Filter{Channel: q.Get("channel"), Status: q.Get("status"), Limit: 50}
	for name := range q {
		switch name {
		case "status", "channel", "from", "to", "limit", "offset", "cursor":
		default:
			problem(w, 400, "unknown query parameter")
			return
		}
		if len(q[name]) != 1 {
			problem(w, 400, "duplicate query parameter")
			return
		}
	}
	if f.Channel != "" && !validChannel(f.Channel) {
		problem(w, 400, "unknown channel")
		return
	}
	if f.Status != "" && !knownStatus(f.Status) {
		problem(w, 400, "unknown status")
		return
	}
	for name, target := range map[string]*int{"limit": &f.Limit, "offset": &f.Offset} {
		if q.Has(name) {
			n, err := strconv.Atoi(q.Get(name))
			if err != nil || n < 0 || name == "limit" && (n < 1 || n > 100) {
				problem(w, 400, "invalid "+name)
				return
			}
			*target = n
		}
	}
	for name, target := range map[string]**time.Time{"from": &f.From, "to": &f.To} {
		if q.Has(name) {
			ts, err := time.Parse(time.RFC3339Nano, q.Get(name))
			if err != nil {
				problem(w, 400, "invalid "+name+" timestamp")
				return
			}
			*target = &ts
		}
	}
	if f.From != nil && f.To != nil && f.From.After(*f.To) {
		problem(w, 400, "from must precede to")
		return
	}
	if q.Has("cursor") {
		f.Before = decodeListCursor(q.Get("cursor"))
		if f.Before == nil || q.Has("offset") {
			problem(w, 400, "supply a valid cursor without offset")
			return
		}
	}
	pageSize := f.Limit
	f.Limit++ // One extra row tells the caller whether there is another page.
	items, err := s.repo.List(r.Context(), f)
	if err != nil {
		repositoryError(w, err)
		return
	}
	if items == nil {
		items = []store.Notification{}
	}
	var nextCursor string
	if len(items) > pageSize {
		items = items[:pageSize]
		nextCursor = encodeListCursor(items[pageSize-1])
	}
	writeJSON(w, 200, struct {
		Items      []store.Notification `json:"items"`
		Limit      int                  `json:"limit"`
		Offset     int                  `json:"offset"`
		NextCursor string               `json:"nextCursor,omitempty"`
	}{items, pageSize, f.Offset, nextCursor})
}

func knownStatus(s string) bool {
	switch s {
	case "pending", "sending", "scheduled", "accepted", "retry_wait", "failed", "unknown", "cancelled":
		return true
	default:
		return false
	}
}

func (s server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.repo.Ping(ctx); err != nil {
		writeJSON(w, 503, map[string]string{"status": "unavailable"})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ok"})
}

func validChannel(s string) bool { return s == "sms" || s == "email" || s == "push" }

func (s server) metrics(w http.ResponseWriter, r *http.Request) {
	m, err := s.repo.Metrics(r.Context())
	if err != nil {
		repositoryError(w, err)
		return
	}
	var out strings.Builder
	fmt.Fprintln(&out, "# TYPE notification_queue_depth gauge")
	fmt.Fprintln(&out, "notification_queue_depth", m.Counts["pending"])
	fmt.Fprintln(&out, "# TYPE notification_oldest_pending_seconds gauge")
	fmt.Fprintln(&out, "notification_oldest_pending_seconds", m.OldestPendingSeconds)
	fmt.Fprintln(&out, "# TYPE notification_state gauge")
	for _, state := range []string{"pending", "scheduled", "retry_wait", "sending", "accepted", "failed", "unknown", "cancelled"} {
		fmt.Fprintf(&out, "notification_state{status=%q} %d\n", state, m.Counts[state])
	}
	writeOutcomeMetrics(&out, m)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = io.WriteString(w, out.String())
}
