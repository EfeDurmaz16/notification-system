package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
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

func TestChunkedCreatesShareCapacityAndReleaseIt(t *testing.T) {
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	handler := limitCreateBodies(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/batches" {
			entered <- struct{}{}
			<-release
		}
		w.WriteHeader(http.StatusAccepted)
	})
	var ongoing sync.WaitGroup
	for range 4 {
		ongoing.Go(func() {
			req := httptest.NewRequest("POST", "/v1/batches", nil)
			req.ContentLength = -1
			handler(httptest.NewRecorder(), req)
		})
	}
	for range 4 {
		<-entered
	}
	req := httptest.NewRequest("POST", "/v1/notifications", nil)
	retry := httptest.NewRecorder()
	handler(retry, req)
	close(release)
	ongoing.Wait()
	if retry.Code != 503 || retry.Header().Get("Retry-After") != "1" {
		t.Fatalf("create at capacity: %d, %+v", retry.Code, retry.Header())
	}
	after := httptest.NewRecorder()
	handler(after, req)
	if after.Code != 202 {
		t.Fatalf("after release: HTTP %d", after.Code)
	}
}
