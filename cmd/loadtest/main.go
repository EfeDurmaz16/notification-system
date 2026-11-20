// Command loadtest measures a running notification API against the local synthetic provider.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
)

type config struct {
	Scenario    string        `json:"scenario"`
	API         string        `json:"api"`
	Provider    string        `json:"provider"`
	Total       int           `json:"offeredNotifications"`
	Batch       int           `json:"notificationsPerRequest"`
	Rate        float64       `json:"baseOfferedNotificationsPerSecond"`
	Concurrency int           `json:"maxInFlightCreates"`
	Poll        time.Duration `json:"pollIntervalNs"`
	Deadline    time.Duration `json:"deadlineNs"`
	Label       string        `json:"label,omitempty"`
}
type latency struct {
	N   int     `json:"n"`
	P50 float64 `json:"p50Ms"`
	P95 float64 `json:"p95Ms"`
	P99 float64 `json:"p99Ms"`
	Max float64 `json:"maxMs"`
}

func summarize(data []float64) latency {
	if len(data) == 0 {
		return latency{}
	}
	slices.Sort(data)
	p := func(q float64) float64 { return data[int(math.Ceil(float64(len(data))*q))-1] }
	return latency{len(data), p(.5), p(.95), p(.99), p(1)}
}
func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

type fixtureStats struct {
	Attempts     int `json:"attempts"`
	Accepted     int `json:"accepted"`
	Dedup        int `json:"deduplicated"`
	Failures     int `json:"failures"`
	ResponseLost int `json:"responseLost"`
	Channels     map[string]struct {
		Attempts int `json:"attempts"`
		Peak     int `json:"peakPerSecond"`
	} `json:"channels"`
}
type sample struct {
	Elapsed     float64 `json:"elapsedSeconds"`
	Accepted    int     `json:"apiAccepted"`
	Terminal    int     `json:"observedTerminal"`
	Outstanding int     `json:"acceptedMinusObservedTerminal"`
}
type result struct {
	RunID             string             `json:"runId"`
	Config            config             `json:"config"`
	Started           time.Time          `json:"startedAt"`
	Platform          string             `json:"platform"`
	CreatedRequests   int                `json:"startedCreateRequests"`
	APIStatuses       map[int]int        `json:"createHTTPStatuses"`
	Accepted          int                `json:"acceptedNotifications"`
	Rejected          int                `json:"validationRejected"`
	Statuses          map[string]int     `json:"finalObservedStates"`
	Observed          int                `json:"observedTerminal"`
	Outstanding       int                `json:"notObservedTerminal"`
	CreateLatency     latency            `json:"createResponseLatency"`
	LaunchLag         latency            `json:"generatorSchedulingDelay"`
	TerminalLatency   latency            `json:"createStartToObservedTerminal"`
	ByPriority        map[string]latency `json:"createStartToPersistedTerminalByPriority"`
	StateLatency      latency            `json:"createStartToPersistedTerminal"`
	PollErrors        int                `json:"pollErrors"`
	Errors            []string           `json:"errors,omitempty"`
	IngressSeconds    float64            `json:"ingressSeconds"`
	RunSeconds        float64            `json:"elapsedSeconds"`
	DrainSeconds      float64            `json:"secondsFromLastCreateResponseToFinalObservation"`
	AcceptedPerSecond float64            `json:"apiAcceptedPerIngressSecond"`
	Throughput        float64            `json:"observedTerminalPerElapsedSecond"`
	Provider          fixtureStats       `json:"provider"`
	Timeline          []sample           `json:"timeline"`
	Checks            map[string]bool    `json:"checks"`
}
type item struct {
	Index   int       `json:"index"`
	ID      string    `json:"id"`
	Error   string    `json:"error"`
	Status  string    `json:"status"`
	Updated time.Time `json:"updatedAt"`
}
type batchResponse struct {
	Batch string `json:"batchId"`
	Items []item `json:"items"`
}
type notif struct {
	Recipient string `json:"recipient"`
	Channel   string `json:"channel"`
	Content   string `json:"content"`
	Priority  string `json:"priority"`
}
type harness struct {
	mu                                      sync.Mutex
	r                                       result
	client                                  *http.Client
	creates, launches, observations, states []float64
	priorities                              map[string][]float64
	lastCreate                              time.Time
	seen                                    map[string]string
	runID                                   string
}

func (h *harness) err(msg string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.r.Errors) < 12 {
		h.r.Errors = append(h.r.Errors, msg)
	}
}

func (h *harness) request(ctx context.Context, method, loc string, data []byte, key string, target any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, method, loc, bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	if data != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return resp.StatusCode, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	if target != nil {
		return resp.StatusCode, json.NewDecoder(resp.Body).Decode(target)
	}
	_, err = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, err
}
func terminal(s string) bool {
	switch s {
	case "accepted", "failed", "unknown", "cancelled":
		return true
	}
	return false
}

func (h *harness) observe(ctx context.Context, id string, start time.Time, expected map[string]string) {
	t := time.NewTicker(h.r.Config.Poll)
	defer t.Stop()
	for len(expected) > 0 {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		var b batchResponse
		_, err := h.request(ctx, "GET", h.r.Config.API+"/v1/batches/"+url.PathEscape(id), nil, "", &b)
		if err != nil {
			h.mu.Lock()
			h.r.PollErrors++
			h.mu.Unlock()
			if ctx.Err() == nil {
				h.err("poll: " + err.Error())
			}
			continue
		}
		now := time.Now()
		h.mu.Lock()
		for _, i := range b.Items {
			priority, ok := expected[i.ID]
			if !ok {
				continue
			}
			h.seen[i.ID] = i.Status
			if !terminal(i.Status) {
				continue
			}
			delete(expected, i.ID)
			h.r.Observed++
			h.observations = append(h.observations, ms(now.Sub(start)))
			if !i.Updated.IsZero() && !i.Updated.Before(start) {
				value := ms(i.Updated.Sub(start))
				h.states = append(h.states, value)
				h.priorities[priority] = append(h.priorities[priority], value)
			}
		}
		h.mu.Unlock()
	}
}
func recipient(channel string, index int) string {
	switch channel {
	case "email":
		return fmt.Sprintf("synthetic-%d@example.invalid", index)
	case "push":
		return fmt.Sprintf("local-device-%d", index)
	default:
		return fmt.Sprintf("+90555%07d", index%10000000)
	}
}

func (h *harness) create(ctx context.Context, offset, n int, due time.Time, wg *sync.WaitGroup) {
	rows := make([]notif, n)
	for i := range rows {
		idx := offset + i
		value := fmt.Sprintf("test %s item %d", h.runID, idx)
		if h.r.Config.Scenario == "failures" {
			switch idx % 3 {
			case 1:
				value = "[mock:reject] " + value
			case 2:
				value = "[mock:unknown] " + value
			}
		}
		channel, priorityIndex := "sms", idx
		if h.r.Config.Scenario == "mixed" {
			channel = []string{"sms", "email", "push"}[idx%3]
			priorityIndex = idx / 3
		}
		rows[i] = notif{recipient(channel, idx), channel, value, []string{"high", "normal", "low"}[priorityIndex%3]}
	}
	data, _ := json.Marshal(map[string]any{"notifications": rows})
	start := time.Now()
	h.mu.Lock()
	h.launches = append(h.launches, ms(start.Sub(due)))
	h.r.CreatedRequests++
	h.mu.Unlock()
	var b batchResponse
	code, err := h.request(ctx, "POST", h.r.Config.API+"/v1/batches", data, fmt.Sprintf("%s-%d", h.runID, offset), &b)
	end := time.Now()
	h.mu.Lock()
	h.creates = append(h.creates, ms(end.Sub(start)))
	h.r.APIStatuses[code]++
	if end.After(h.lastCreate) {
		h.lastCreate = end
	}
	h.mu.Unlock()
	if err != nil {
		h.err("create: " + err.Error())
		return
	}
	if code != 202 || b.Batch == "" || len(b.Items) != n {
		h.err("incomplete create receipt")
		return
	}
	expected := make(map[string]string)
	h.mu.Lock()
	for _, i := range b.Items {
		if i.Error != "" {
			h.r.Rejected++
			continue
		}
		if i.ID == "" || i.Index < 0 || i.Index >= len(rows) {
			continue
		}
		expected[i.ID] = rows[i.Index].Priority
		h.r.Accepted++
		h.seen[i.ID] = "pending"
	}
	h.mu.Unlock()
	wg.Add(1)
	go func() { defer wg.Done(); h.observe(ctx, b.Batch, start, expected) }()
}
func offeredRate(c config, offset int) float64 {
	if c.Scenario != "stress" {
		return c.Rate
	}
	// Three equal-size stages. With a base of 75/s this offers 75, 150, then 300/s.
	return c.Rate * []float64{1, 2, 4}[min(2, 3*offset/c.Total)]
}
func (h *harness) run(ctx context.Context) error {
	c := h.r.Config
	h.r.RunID = h.runID
	h.priorities = make(map[string][]float64)
	if _, err := h.request(ctx, "DELETE", c.Provider+"/stats", nil, "", nil); err != nil {
		return fmt.Errorf("reset isolated provider: %w", err)
	}
	h.r.Started = time.Now().UTC()
	h.lastCreate = h.r.Started
	samplesCtx, stop := context.WithCancel(ctx)
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-samplesCtx.Done():
				return
			case <-t.C:
				h.mu.Lock()
				h.r.Timeline = append(h.r.Timeline, sample{time.Since(h.r.Started).Seconds(), h.r.Accepted, h.r.Observed, h.r.Accepted - h.r.Observed})
				h.mu.Unlock()
			}
		}
	}()
	var creates, polls sync.WaitGroup
	permits := make(chan struct{}, c.Concurrency)
	due := h.r.Started
	for offset := 0; offset < c.Total && ctx.Err() == nil; {
		n := min(c.Batch, c.Total-offset)
		if d := time.Until(due); d > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(d):
			}
		}
		select {
		case <-ctx.Done():
		case permits <- struct{}{}:
			creates.Add(1)
			go func(off, count int, target time.Time) {
				defer creates.Done()
				defer func() { <-permits }()
				h.create(ctx, off, count, target, &polls)
			}(offset, n, due)
		}
		if c.Rate > 0 {
			due = due.Add(time.Duration(float64(time.Second) * float64(n) / offeredRate(c, offset)))
		}
		offset += n
	}
	creates.Wait()
	polls.Wait()
	stop()
	sampler.Wait()
	end := time.Now()
	h.r.IngressSeconds = h.lastCreate.Sub(h.r.Started).Seconds()
	h.r.RunSeconds = end.Sub(h.r.Started).Seconds()
	h.r.DrainSeconds = end.Sub(h.lastCreate).Seconds()
	h.r.Outstanding = h.r.Accepted - h.r.Observed
	h.r.CreateLatency = summarize(h.creates)
	h.r.LaunchLag = summarize(h.launches)
	h.r.TerminalLatency = summarize(h.observations)
	h.r.StateLatency = summarize(h.states)
	h.r.ByPriority = make(map[string]latency)
	for tier, samples := range h.priorities {
		h.r.ByPriority[tier] = summarize(samples)
	}
	if h.r.IngressSeconds > 0 {
		h.r.AcceptedPerSecond = float64(h.r.Accepted) / h.r.IngressSeconds
	}
	if h.r.RunSeconds > 0 {
		h.r.Throughput = float64(h.r.Observed) / h.r.RunSeconds
	}
	for _, v := range h.seen {
		h.r.Statuses[v]++
	}
	h.r.Timeline = append(h.r.Timeline, sample{h.r.RunSeconds, h.r.Accepted, h.r.Observed, h.r.Outstanding})
	finalCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := h.request(finalCtx, "GET", c.Provider+"/stats", nil, "", &h.r.Provider)
	if err != nil {
		h.err("get fixture stats: " + err.Error())
	}
	h.r.Checks["fixtureStatsAvailable"] = err == nil
	h.r.Checks["allInputsAccepted"] = h.r.Accepted == c.Total && h.r.Rejected == 0
	h.r.Checks["allReachedTerminal"] = h.r.Observed == c.Total
	h.r.Checks["oneProviderAttemptPerInput"] = h.r.Provider.Attempts == c.Total
	expectedAccepted := c.Total
	if c.Scenario == "failures" {
		expectedAccepted = (c.Total + 2) / 3
		h.r.Checks["permanentFailuresClassified"] = h.r.Statuses["failed"] == (c.Total+1)/3
		h.r.Checks["ambiguousResponsesClassified"] = h.r.Statuses["unknown"] == c.Total/3
	}
	h.r.Checks["expectedAcceptedState"] = h.r.Statuses["accepted"] == expectedAccepted
	expectedChannels := map[string]int{"sms": c.Total}
	if c.Scenario == "mixed" {
		expectedChannels = map[string]int{"sms": (c.Total + 2) / 3, "email": (c.Total + 1) / 3, "push": c.Total / 3}
	}
	covered, withinQuota := true, true
	for channel, want := range expectedChannels {
		got := h.r.Provider.Channels[channel]
		covered = covered && got.Attempts == want
		withinQuota = withinQuota && got.Peak <= 100
	}
	h.r.Checks["expectedAttemptsPerChannel"] = covered
	h.r.Checks["providerRollingSecondWithinQuota"] = covered && withinQuota && err == nil
	return ctx.Err()
}
func run() int {
	var c config
	var output string
	flag.StringVar(&c.Scenario, "scenario", "burst", "burst, load, stress, failures, or mixed")
	flag.StringVar(&c.API, "api", "http://127.0.0.1:8080", "API URL")
	flag.StringVar(&c.Provider, "provider", "http://127.0.0.1:8090", "isolated mock provider URL, stats are reset")
	flag.IntVar(&c.Total, "total", 0, "total notifications")
	flag.IntVar(&c.Batch, "batch", 0, "notifications per request (1 to 1000)")
	flag.Float64Var(&c.Rate, "rate", -1, "offered notifications/s, zero for concurrent burst")
	flag.IntVar(&c.Concurrency, "concurrency", 32, "max concurrent create requests")
	flag.DurationVar(&c.Poll, "poll", 250*time.Millisecond, "status polling interval per batch")
	flag.DurationVar(&c.Deadline, "deadline", 3*time.Minute, "overall deadline")
	flag.StringVar(&c.Label, "label", "", "deployment configuration/replica label")
	flag.StringVar(&output, "out", "", "optional pretty JSON output file")
	flag.Parse()
	defaults, ok := map[string]struct {
		total int
		batch int
		rate  float64
	}{
		"load": {600, 10, 50}, "stress": {1200, 20, 75}, "burst": {1000, 1000, 0},
		"failures": {30, 10, 20}, "mixed": {3000, 300, 0},
	}[c.Scenario]
	if !ok {
		fmt.Fprintln(os.Stderr, "invalid scenario")
		return 2
	}
	if c.Total == 0 {
		c.Total = defaults.total
	}
	if c.Batch == 0 {
		c.Batch = defaults.batch
	}
	if c.Rate == -1 {
		c.Rate = defaults.rate
	}
	if c.Total < 1 || c.Batch < 1 || c.Batch > 1000 || c.Poll <= 0 || c.Concurrency < 1 || c.Deadline <= 0 || math.IsNaN(c.Rate) || math.IsInf(c.Rate, 0) || c.Rate < 0 {
		fmt.Fprintln(os.Stderr, "invalid workload configuration")
		return 2
	}
	c.API = strings.TrimRight(c.API, "/")
	c.Provider = strings.TrimRight(c.Provider, "/")
	for _, addr := range []string{c.API, c.Provider} {
		u, err := url.Parse(addr)
		if err != nil || u.Scheme != "http" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
			fmt.Fprintln(os.Stderr, "use local HTTP endpoints for synthetic experiments")
			return 2
		}
	}
	h := harness{r: result{Config: c, Platform: runtime.Version() + " " + runtime.GOOS + "/" + runtime.GOARCH, APIStatuses: map[int]int{}, Statuses: map[string]int{}, Checks: map[string]bool{}}, client: &http.Client{Timeout: 15 * time.Second}, seen: map[string]string{}, runID: rand.Text()}
	ctx, cancel := context.WithTimeout(context.Background(), c.Deadline)
	defer cancel()
	err := h.run(ctx)
	if err != nil {
		h.err(err.Error())
	}
	raw, errJSON := json.MarshalIndent(h.r, "", "  ")
	if errJSON != nil {
		fmt.Fprintln(os.Stderr, errJSON)
		return 2
	}
	raw = append(raw, '\n')
	if output != "" {
		if err := os.WriteFile(output, raw, 0644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
	}
	_, _ = os.Stdout.Write(raw)
	if err != nil {
		return 1
	}
	for _, passed := range h.r.Checks {
		if !passed {
			return 1
		}
	}
	return 0
}
func main() { os.Exit(run()) }
