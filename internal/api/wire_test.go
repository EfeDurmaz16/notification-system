package api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/EfeDurmaz16/notification-system/internal/store"
)

func TestAmbiguousJSONNeverReservesKey(t *testing.T) {
	bad := []string{
		`"priority":"high","Priority":"bogus"`,
		`"Priority":"bogus","priority":"high"`,
		`"priority":"high","priority":"low"`,
		`"prio\u0072ity":"low","priority":"high"`,
		`"prioritY":"high"`,
		`"priority":"high","variables":{"x":"1","x":"2"}`,
	}
	for _, endpoint := range []string{"/v1/notifications", "/v1/batches"} {
		for _, fields := range bad {
			t.Run(endpoint+"/"+fields, func(t *testing.T) {
				repo := new(repositoryStub)
				h := New(repo)
				bad := strings.Replace(validInput, `"priority":"normal"`, fields, 1)
				good := validInput
				if endpoint == "/v1/batches" {
					bad = `{"notifications":[` + validInput + "," + bad + "]}"
					good = `{"notifications":[` + validInput + "]}"
				}
				if w := call(h, "POST", endpoint, bad, "same"); w.Code != http.StatusBadRequest {
					t.Fatalf("ambiguous body accepted: status %d, receipt %s", w.Code, w.Body.String())
				}
				if repo.hash != nil {
					t.Fatal("ambiguous request reached storage")
				}
				if w := call(h, "POST", endpoint, good, "same"); w.Code != 202 {
					t.Fatalf("key unavailable: %d", w.Code)
				}
			})
		}
	}
}
func TestUnicodeInputBoundary(t *testing.T) {
	for _, example := range []struct {
		content string
		status  int
	}{
		{`\ud83d\ude80`, 202}, {`\\ud800`, 202}, {`\uFFFD`, 202}, {`\uD800`, 400}, {`\uDC00`, 400}, {`\ud800\u0041`, 400},
		{string([]byte{255}), 400},
	} {
		r := new(repositoryStub)
		w := call(New(r), "POST", "/v1/notifications", strings.Replace(validInput, "hello", example.content, 1), "key")
		if w.Code != example.status {
			t.Errorf("content %q got %d want %d", example.content, w.Code, example.status)
		}
	}
}

func TestCreateUsesStoredReceiptForStatus(t *testing.T) {
	// Simulate replay of receipts produced under previous validation rules.
	for _, p := range []string{"/v1/notifications", "/v1/batches"} {
		for _, tc := range []struct {
			currentValid bool
			stored       store.ItemResult
			want         int
		}{
			{true, store.ItemResult{Error: "previously invalid"}, 422},
			{false, store.ItemResult{ID: validID, Status: "pending"}, 202},
		} {
			body := validInput
			if !tc.currentValid {
				body = "{}"
			}
			if p == "/v1/batches" {
				body = `{"notifications":[` + body + "]}"
			}
			_, h, err := requestItems(p, []byte(body))
			if err != nil {
				t.Fatal(err)
			}
			s := &repositoryStub{hash: h, result: store.BatchResult{BatchID: validID, Items: []store.ItemResult{tc.stored}}}
			if w := call(New(s), "POST", p, body, "key"); w.Code != tc.want {
				t.Errorf("route %s currentValid=%v got %d want %d", p, tc.currentValid, w.Code, tc.want)
			}
		}
	}
}
