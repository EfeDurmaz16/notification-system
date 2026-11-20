// Command provider runs a synthetic HTTP notification provider for local experiments.
package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type payload struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type receipt struct {
	MessageID string    `json:"messageId"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

type cacheEntry struct {
	Payload payload
	Receipt receipt
}

type channelStats struct {
	Attempts int `json:"attempts"`
	Peak     int `json:"peakPerSecond"`
	recent   []time.Time
}

func (s *channelStats) record(now time.Time) {
	cutoff := now.Add(-time.Second)
	for len(s.recent) > 0 && !s.recent[0].After(cutoff) {
		s.recent = s.recent[1:]
	}
	s.recent = append(s.recent, now)
	s.Attempts++
	s.Peak = max(s.Peak, len(s.recent))
}

type stats struct {
	Attempts     int                      `json:"attempts"`
	Accepted     int                      `json:"accepted"`
	Dedup        int                      `json:"deduplicated"`
	Failures     int                      `json:"failures"`
	ResponseLost int                      `json:"responseLost"`
	Channels     map[string]*channelStats `json:"channels"`
}
type fixture struct {
	mu         sync.Mutex
	stats      stats
	seen       map[string]cacheEntry
	byID       map[string]int
	busy       map[[32]byte]bool
	idempotent bool
	latency    time.Duration
}

func (f *fixture) reset() {
	f.stats = stats{Channels: make(map[string]*channelStats)}
	f.seen = make(map[string]cacheEntry)
	f.byID = make(map[string]int)
	f.busy = make(map[[32]byte]bool)
}

func respond(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func (f *fixture) send(w http.ResponseWriter, r *http.Request) {
	var p payload
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&p); err != nil || p.To == "" || p.Content == "" || (p.Channel != "sms" && p.Channel != "email" && p.Channel != "push") {
		http.Error(w, "invalid provider payload", http.StatusBadRequest)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	f.mu.Lock()
	c := f.stats.Channels[p.Channel]
	if c == nil {
		c = &channelStats{}
		f.stats.Channels[p.Channel] = c
	}
	c.record(time.Now())
	f.stats.Attempts++
	if id := r.Header.Get("X-Notification-ID"); id != "" {
		f.byID[id]++
	}
	if cached, ok := f.seen[key]; f.idempotent && key != "" && ok {
		if p != cached.Payload {
			f.stats.Failures++
			f.mu.Unlock()
			http.Error(w, "idempotency key conflicts with existing payload", http.StatusConflict)
			return
		}
		f.stats.Dedup++
		f.mu.Unlock()
		respond(w, http.StatusAccepted, cached.Receipt)
		return
	}
	if strings.HasPrefix(p.Content, "[mock:reject]") {
		f.stats.Failures++
		f.mu.Unlock()
		http.Error(w, "invalid recipient", http.StatusBadRequest)
		return
	}
	if strings.HasPrefix(p.Content, "[mock:busy-once]") {
		fingerprint := sha256.Sum256([]byte(p.Channel + "\x00" + p.To + "\x00" + p.Content))
		if !f.busy[fingerprint] {
			f.busy[fingerprint] = true
			f.stats.Failures++
			f.mu.Unlock()
			w.Header().Set("Retry-After", "1")
			http.Error(w, "try again later", http.StatusTooManyRequests)
			return
		}
	}
	rcpt := receipt{MessageID: newID(), Status: "accepted", Timestamp: time.Now().UTC()}
	f.stats.Accepted++
	if f.idempotent && key != "" {
		f.seen[key] = cacheEntry{Payload: p, Receipt: rcpt}
	}
	lose := strings.HasPrefix(p.Content, "[mock:unknown]")
	if lose {
		f.stats.ResponseLost++
	}
	f.mu.Unlock()
	if f.latency > 0 {
		select {
		case <-time.After(f.latency):
		case <-r.Context().Done():
			return
		}
	}
	if lose {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			_ = conn.Close()
			return
		}
		panic(http.ErrAbortHandler)
	}
	respond(w, http.StatusAccepted, rcpt)
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (f *fixture) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /send", f.send)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { respond(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		respond(w, 200, f.stats)
	})
	mux.HandleFunc("GET /stats/{id}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		n := f.byID[r.PathValue("id")]
		f.mu.Unlock()
		respond(w, 200, map[string]int{"attempts": n})
	})
	mux.HandleFunc("DELETE /stats", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		f.reset()
		f.mu.Unlock()
		w.WriteHeader(204)
	})
	return mux
}

func main() {
	addr := os.Getenv("PROVIDER_ADDR")
	if addr == "" {
		addr = ":8081"
	}
	delay := 20 * time.Millisecond
	if v := os.Getenv("PROVIDER_LATENCY"); v != "" {
		var err error
		delay, err = time.ParseDuration(v)
		if err != nil || delay < 0 {
			log.Fatal("invalid PROVIDER_LATENCY")
		}
	}
	f := &fixture{latency: delay, idempotent: os.Getenv("PROVIDER_IDEMPOTENT") == "true"}
	f.reset()
	s := http.Server{Addr: addr, Handler: f.routes(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	log.Printf("synthetic provider listening on %s, delay=%s, idempotent=%t", addr, delay, f.idempotent)
	if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
