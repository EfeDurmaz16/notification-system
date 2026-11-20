package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func fixtureBody(content string) string {
	return `{"to":"+905550001234","channel":"sms","content":"` + content + `"}`
}

func TestWindowBoundaries(t *testing.T) {
	var s channelStats
	start := time.Unix(1700000000, 0)
	for i := 0; i < 100; i++ {
		s.record(start)
	}
	s.record(start.Add(time.Second - time.Nanosecond))
	s.record(start.Add(time.Second))
	if s.Peak != 101 || len(s.recent) != 2 || s.Attempts != 102 {
		t.Fatalf("wrong window: %+v", s)
	}
}

func TestIdempotencyAndUnknown(t *testing.T) {
	f := &fixture{idempotent: true}
	f.reset()
	srv := httptest.NewServer(f.routes())
	defer srv.Close()
	post := func(content string) (receipt, error) {
		req, _ := http.NewRequest("POST", srv.URL+"/send", strings.NewReader(fixtureBody(content)))
		req.Header.Set("Idempotency-Key", "operation-1")
		req.Header.Set("X-Notification-ID", "n1")
		resp, err := srv.Client().Do(req)
		if err != nil {
			return receipt{}, err
		}
		defer resp.Body.Close()
		var r receipt
		err = json.NewDecoder(resp.Body).Decode(&r)
		return r, err
	}
	if _, err := post("[mock:unknown] demo"); err == nil {
		t.Fatal("first response should be lost")
	}
	first, err := post("[mock:unknown] demo")
	if err != nil {
		t.Fatal(err)
	}
	second, err := post("[mock:unknown] demo")
	if err != nil || first.MessageID == "" || first != second {
		t.Fatalf("replay mismatch %+v %+v %v", first, second, err)
	}
	resp, err := srv.Client().Get(srv.URL + "/stats/n1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.TrimSpace(string(body)) != `{"attempts":3}` {
		t.Fatalf("expected three independent attempts: %s", body)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stats.Accepted != 1 || f.stats.Dedup != 2 || f.stats.ResponseLost != 1 {
		t.Fatalf("wrong stats: %+v", f.stats)
	}
}

func BenchmarkWindowAccounting(b *testing.B) {
	b.ReportAllocs()
	var s channelStats
	start := time.Unix(1700000000, 0)
	i := 0
	for b.Loop() {
		s.record(start.Add(time.Duration(i) * 10 * time.Millisecond))
		i++
	}
}
