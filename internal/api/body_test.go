package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/store"
)

// Old receipts store the digest of encoding/json's sorted generic document.
// Keep a reference implementation here so upgrades check the exact wire bytes.
func TestPersistedFingerprintCompatibility(t *testing.T) {
	for _, item := range []string{
		validInput,
		`{"recipient":"<a>&","channel":"email","variables":{"x":"\u2028","\u0061":"\ud83d\ude80","c":"a\\\"b"},"other":[-0,1e+07,true,false,null,{}]}`,
		`{"escaped\/key":"foo\/bar","other":[1,{"a":"}]],","b":["[","{"]},"","  ",[],{}],"ç":"\\\u0041"}`,
		`[1,2,[{"ñ":"¡", "a":"a"}]]`,
	} {
		for _, path := range []string{"/v1/batches", "/v1/notifications"} {
			raw := item
			if path == "/v1/batches" {
				raw = `  {"notifi\u0063ations": [ ` + item + ` , {"variables":{}} ] } `
			}
			var original any
			dec := json.NewDecoder(strings.NewReader(raw))
			dec.UseNumber()
			if err := dec.Decode(&original); err != nil {
				t.Fatal(err)
			}
			h := sha256.New()
			_, _ = fmt.Fprintln(h, path)
			if err := json.NewEncoder(h).Encode(original); err != nil {
				t.Fatal(err)
			}
			_, got, err := requestItems(path, []byte(raw))
			if err != nil || !bytes.Equal(h.Sum(nil), got) {
				t.Fatalf("fingerprint mismatch for %s %s: %x != %x (%v)", path, raw, got, h.Sum(nil), err)
			}
		}
	}
}

func TestBatchWireErrorIncludesIndexAndLeavesKeyFree(t *testing.T) {
	repo := new(repositoryStub)
	body := `{"notifications":[` + validInput + `,{"variables":{"a":"x","\u0061":"y"}}]}`
	w := call(New(repo), "POST", "/v1/batches", body, "correctable")
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "notifications[1]") || repo.hash != nil {
		t.Fatalf("got HTTP %d: %s", w.Code, w.Body.String())
	}
}

type deadlineRepository struct {
	repositoryStub
	timeout time.Duration
}

func (repo *deadlineRepository) Get(ctx context.Context, id string) (store.Notification, error) {
	if deadline, ok := ctx.Deadline(); ok {
		repo.timeout = time.Until(deadline)
	}
	return repo.repositoryStub.Get(ctx, id)
}

func TestDatabaseReadHasDeadline(t *testing.T) {
	repo := new(deadlineRepository)
	call(New(repo), "GET", "/v1/notifications/"+validID, "", "")
	if repo.timeout <= 0 || repo.timeout > 25*time.Second {
		t.Fatalf("repository operation deadline is %s", repo.timeout)
	}
}

// A full supported batch with many template variables. Report allocations for
// valid submissions without contacting a database, provider or network service.
func BenchmarkBatchTemplateSubmission(b *testing.B) {
	vars := make(map[string]string)
	for i := range 420 {
		vars[fmt.Sprintf("v%03d", i)] = "a"
	}
	mapping, err := json.Marshal(vars)
	if err != nil {
		b.Fatal(err)
	}
	item := strings.Replace(validInput, `"content":"hello"`, `"template":"Hello {{v001}}","variables":`+string(mapping), 1)
	body := `{"notifications":[` + strings.Repeat(item+",", 999) + item + `]}`
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		w := call(New(new(repositoryStub)), "POST", "/v1/batches", body, "bench")
		if w.Code != 202 {
			b.Fatalf("HTTP %d", w.Code)
		}
	}
}

// A reader signals when the handler is waiting for the remainder of a valid
// upload. The test uses a small shared budget and no sockets or wall-clock sleeps.
type pausedUpload struct {
	prefix, rest *strings.Reader
	waiting      chan struct{}
	resume       chan struct{}
}

func (r *pausedUpload) Read(p []byte) (int, error) {
	if r.prefix.Len() > 0 {
		return r.prefix.Read(p)
	}
	if r.waiting != nil {
		close(r.waiting)
		r.waiting = nil
		<-r.resume
	}
	return r.rest.Read(p)
}

func TestUploadsShareOnlyReceivedBodyMemory(t *testing.T) {
	for _, length := range []int64{-1, int64(len(validInput)), maxBody} {
		waiting := make(chan struct{})
		upload := &pausedUpload{
			prefix: strings.NewReader(validInput[:20]), rest: strings.NewReader(validInput[20:]),
			waiting: waiting, resume: make(chan struct{}),
		}
		budget := &bodyBudget{available: 12 << 10}
		// Only read the body here: the two requests have independent store state.
		first := createBody{budget: budget}
		req := httptest.NewRequest("POST", "/v1/notifications", upload)
		req.ContentLength = length
		done := make(chan bool, 1)
		go func() {
			ok := first.readRequest(httptest.NewRecorder(), req)
			first.release()
			done <- ok
		}()
		select {
		case <-waiting:
		case <-done:
			t.Fatalf("upload length %d was rejected before reading the prefix", length)
		case <-time.After(time.Second * 5):
			t.Fatal("upload did not start")
		}
		// Reading into the initial buffer costs the same for either framing.
		availableWhilePaused := budget.available
		second := createBody{budget: budget}
		other := httptest.NewRequest("POST", "/v1/notifications", strings.NewReader(validInput))
		ok := second.readRequest(httptest.NewRecorder(), other)
		second.release()
		close(upload.resume)
		if !<-done || !ok || availableWhilePaused != (12<<10)-initialBodyCapacity || budget.available != 12<<10 {
			t.Fatalf("length %d: concurrent upload=%t, available during=%d after=%d", length, ok, availableWhilePaused, budget.available)
		}
	}
}

func TestCreateCapacityErrorReleasesBudgetAndKey(t *testing.T) {
	repo := new(repositoryStub)
	budget := &bodyBudget{available: initialBodyCapacity - 1}
	handler := http.HandlerFunc(server{repo: repo, bodies: budget}.create)
	const key = "retry-when-capacity-returns"
	w := call(handler, "POST", "/v1/notifications", validInput, key)
	if w.Code != 503 || w.Header().Get("Retry-After") != "1" || repo.hash != nil {
		t.Fatalf("capacity rejection: HTTP %d, headers %v, key reserved=%t", w.Code, w.Header(), repo.hash != nil)
	}
	budget.available = 1 << 13
	// A failed read must also return the capacity of its partial buffer.
	bad := httptest.NewRequest("POST", "/v1/notifications", io.MultiReader(strings.NewReader("{"), strings.NewReader("}")))
	bad.Header.Set("Idempotency-Key", "invalid")
	bad.Body = io.NopCloser(io.MultiReader(bad.Body, iotest.ErrReader(io.ErrUnexpectedEOF)))
	handler.ServeHTTP(httptest.NewRecorder(), bad)
	if w := call(handler, "POST", "/v1/notifications", validInput, key); w.Code != 202 || budget.available != 1<<13 {
		t.Fatalf("after release: HTTP %d, available %d", w.Code, budget.available)
	}
}

// Pause each upload at the last growth boundary. All four uploads contend
// for 4 MiB -> 5 MiB growth together through the real HTTP server.
type growthBarrier struct {
	io.ReadCloser
	remaining int
	arrived   chan<- struct{}
	resume    <-chan struct{}
}

func (b *growthBarrier) Read(p []byte) (int, error) {
	if b.remaining == 0 && b.arrived != nil {
		b.arrived <- struct{}{}
		b.arrived = nil
		<-b.resume
	}
	if b.remaining > 0 {
		p = p[:min(b.remaining, len(p))]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= n
	return n, err
}

type holdingCreates struct {
	repositoryStub
	arrived chan<- struct{}
	resume  <-chan struct{}
}

func (r *holdingCreates) Create(ctx context.Context, _ string, _ []byte, _ store.Ingress, _ []store.Item) (store.BatchResult, error) {
	r.arrived <- struct{}{}
	select {
	case <-r.resume:
		return store.BatchResult{Items: []store.ItemResult{{ID: validID, Status: "pending"}}}, nil
	case <-ctx.Done():
		return store.BatchResult{}, ctx.Err()
	}
}

func TestFourFullUploadsCanGrowTogether(t *testing.T) {
	const uploads = 4
	atGrowth, atStore := make(chan struct{}, uploads), make(chan struct{}, uploads+1)
	resumeGrowth, resumeStore := make(chan struct{}), make(chan struct{})
	repo := &holdingCreates{arrived: atStore, resume: resumeStore}
	handler := New(repo)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Idempotency-Key") != "fifth" {
			r.Body = &growthBarrier{ReadCloser: r.Body, remaining: 4 << 20, arrived: atGrowth, resume: resumeGrowth}
		}
		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()
	unblockGrowth := sync.OnceFunc(func() { close(resumeGrowth) })
	unblockStore := sync.OnceFunc(func() { close(resumeStore) })
	defer unblockGrowth()
	defer unblockStore()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	body := strings.Repeat(" ", maxBody-len(validInput)) + validInput
	type result struct {
		status     int
		retryAfter string
		err        error
	}
	post := func(key string, chunked bool) result {
		req, err := http.NewRequestWithContext(ctx, "POST", srv.URL+"/v1/notifications", strings.NewReader(body))
		if err != nil {
			return result{err: err}
		}
		if chunked {
			req.ContentLength = -1
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		resp, err := srv.Client().Do(req)
		if err != nil {
			return result{err: err}
		}
		defer func() { _ = resp.Body.Close() }()
		_, err = io.Copy(io.Discard, resp.Body)
		return result{status: resp.StatusCode, retryAfter: resp.Header.Get("Retry-After"), err: err}
	}
	results := make(chan result, uploads)
	for i := range uploads {
		go func() { results <- post(fmt.Sprint(i), i%2 != 0) }()
	}
	waitForFour := func(arrived <-chan struct{}) {
		t.Helper()
		for range uploads {
			select {
			case <-arrived:
			case r := <-results:
				t.Fatalf("upload finished before all four reached the barrier: %+v", r)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}
	waitForFour(atGrowth)
	unblockGrowth()
	waitForFour(atStore)
	if r := post("fifth", false); r.status != http.StatusServiceUnavailable || r.retryAfter != "1" || r.err != nil {
		t.Fatalf("fifth full upload: %+v", r)
	}
	unblockStore()
	for range uploads {
		if r := <-results; r.status != http.StatusAccepted || r.err != nil {
			t.Fatalf("one of four concurrent full uploads: %+v", r)
		}
	}
	if r := post("fifth", false); r.status != http.StatusAccepted || r.err != nil {
		t.Fatalf("retry after releasing capacity: %+v", r)
	}
}
