package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/store"
)

func TestRecipientAndContentRules(t *testing.T) {
	for _, tc := range []struct {
		name, channel, recipient, content, priority string
		accepted                                    bool
	}{
		{"SMS shortest E.164", "sms", "+12", "hello", "low", true},
		{"SMS max digits", "sms", "+" + strings.Repeat("1", 15), "hello", "high", true},
		{"SMS too many digits", "sms", "+" + strings.Repeat("1", 16), "hello", "high", false},
		{"SMS missing plus", "sms", "12345", "hello", "normal", false},
		{"SMS country code zero", "sms", "+0123", "hello", "normal", false},
		{"email at limit", "email", "ada@example.com", strings.Repeat("界", 10000), "normal", true},
		{"email over limit", "email", "ada@example.com", strings.Repeat("界", 10001), "normal", false},
		{"email without domain", "email", "ada", "hello", "normal", false},
		{"email with display name", "email", "Ada <ada@example.com>", "hello", "normal", false},
		{"email address 254 bytes", "email", strings.Repeat("a", 64) + "@" + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61), "hello", "normal", true},
		{"email address too long", "email", strings.Repeat("a", 245) + "@example.com", "hello", "normal", false},
		{"push at limits", "push", strings.Repeat("a", 512), strings.Repeat("界", 256), "high", true},
		{"push over limit", "push", "token", strings.Repeat("界", 257), "high", false},
		{"push token too long", "push", strings.Repeat("a", 513), "hello", "high", false},
		{"empty push token", "push", "", "hello", "high", false},
		{"space in push token", "push", "device token", "hello", "normal", false},
		{"control in push token", "push", "device\x01token", "hello", "normal", false},
		{"unsupported channel", "fax", "+12125551212", "hello", "normal", false},
		{"unsupported priority", "sms", "+12125551212", "hello", "urgent", false},
		{"blank content", "sms", "+12125551212", " \t", "high", false},
		{"interior NUL in content", "sms", "+12125551212", "hello\x00there", "normal", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]string{
				"channel": tc.channel, "recipient": tc.recipient, "content": tc.content, "priority": tc.priority,
			})
			if err != nil {
				t.Fatal(err)
			}
			w := call(New(new(repositoryStub)), "POST", "/v1/notifications", string(raw), "contract")
			want := http.StatusUnprocessableEntity
			if tc.accepted {
				want = http.StatusAccepted
			}
			if w.Code != want {
				t.Fatalf("got %d, want %d: %s", w.Code, want, w.Body.String())
			}
		})
	}
}

func TestTemplateInputRules(t *testing.T) {
	for _, tc := range []struct {
		name, fields string
		want         int
	}{
		{"plain content and template", `"content":"hello","template":"hello"`, 422},
		{"plain content and variables", `"content":"hello","variables":{"x":"y"}`, 422},
		{"missing content and template", `"variables":{}`, 422},
		{"wrong variable type", `"template":"Hello {{x}}","variables":{"x":true}`, 422},
		{"null variable", `"template":"Hello {{x}}","variables":{"x":null}`, 422},
		{"unused null variable", `"template":"Hello","variables":{"x":null}`, 422},
		{"null variables object", `"template":"Hello","variables":null`, 422},
		{"empty string variable", `"template":"Hello{{x}}","variables":{"x":""}`, 202},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.Replace(validInput, `"content":"hello"`, tc.fields, 1)
			w := call(New(new(repositoryStub)), "POST", "/v1/notifications", body, "template")
			if w.Code != tc.want {
				t.Fatalf("got %d, want %d: %s", w.Code, tc.want, w.Body.String())
			}
		})
	}
}

func TestMixedBatchScheduledRange(t *testing.T) {
	dates := []struct {
		when  string
		valid bool
	}{
		{"0000-12-31T23:00:00Z", false},
		{"0001-01-01T00:00:00+01:00", false},
		{"0001-01-01T00:00:00Z", true},
		{"9999-12-31T23:00:00Z", true},
		{"9999-12-31T23:59:59.9999999Z", false},
		{"9999-12-31T23:59:59-01:00", false},
	}
	var items []string
	for _, tc := range dates {
		items = append(items, strings.Replace(validInput, `"content"`, `"scheduledAt":"`+tc.when+`","content"`, 1))
	}
	w := call(New(new(repositoryStub)), "POST", "/v1/batches", `{"notifications":[`+strings.Join(items, ",")+`]}`, "dates")
	var receipt store.BatchResult
	if w.Code != 202 || json.Unmarshal(w.Body.Bytes(), &receipt) != nil || len(receipt.Items) != len(dates) {
		t.Fatalf("mixed batch got %d: %s", w.Code, w.Body.String())
	}
	for i, entry := range receipt.Items {
		if entry.Index != i || (entry.ID != "") != dates[i].valid || (entry.Error == "") != dates[i].valid {
			t.Errorf("scheduledAt %s: %+v", dates[i].when, entry)
		}
	}
}

func TestListFilterContract(t *testing.T) {
	r := new(repositoryStub)
	h := New(r)
	w := call(h, "GET", "/v1/notifications?channel=push&status=unknown&from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z&limit=100&offset=42", "", "")
	f := r.filter
	var page struct{ Limit, Offset int }
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &page) != nil || page.Limit != 100 || page.Offset != 42 ||
		f.Offset != 42 || f.Status != "unknown" || f.Channel != "push" ||
		f.From == nil || f.From.Format(time.DateOnly) != "2026-01-01" || f.To == nil || f.To.Format(time.DateOnly) != "2026-02-01" {
		t.Fatalf("status %d, filter %+v", w.Code, f)
	}
	w = call(h, "GET", "/v1/notifications?from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z", "", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("inverted interval: %d", w.Code)
	}
}

func TestBatchItemSchemaBoundaries(t *testing.T) {
	// All lengths below pass the email parser. The API additionally caps the
	// complete address at 254 bytes, independent of individual label length.
	longEmail := strings.Repeat("a", 64) + "@" + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 62)
	if len(longEmail) != 255 {
		t.Fatalf("fixture length=%d", len(longEmail))
	}
	for _, tc := range []struct{ name, item string }{
		{"missing priority", strings.Replace(validInput, `,"priority":"normal"`, "", 1)},
		{"unknown field with valid required fields", strings.Replace(validInput, `"content":"hello"`, `"content":"hello","unexpected":"x"`, 1)},
		{"255-byte email", strings.Replace(strings.Replace(validInput, `"sms"`, `"email"`, 1), "+905550000111", longEmail, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := new(repositoryStub)
			w := call(New(repo), "POST", "/v1/batches", `{"notifications":[`+validInput+","+tc.item+`]}`, "boundary")
			if w.Code != 202 || len(repo.items) != 2 || repo.items[0].Error != "" || repo.items[1].Error == "" {
				t.Fatalf("HTTP %d, items=%+v", w.Code, repo.items)
			}
		})
	}
}

func TestPlaceholderNameGrammar(t *testing.T) {
	for _, tc := range []struct {
		name  string
		valid bool
	}{
		{"_a1", true}, {"A", true}, {strings.Repeat("a", 64), true},
		{"1abc", false}, {"two-words", false}, {"a.b", false}, {strings.Repeat("a", 65), false},
	} {
		// Supply values for even invalid names so the missing-variable rule
		// cannot mask a broken placeholder-name check.
		got, err := render("{{"+tc.name+"}}", map[string]string{tc.name: "value"}, 160)
		if (err == nil) != tc.valid || tc.valid && got != "value" {
			t.Errorf("name %q rendered %q, err=%v", tc.name, got, err)
		}
	}
}
