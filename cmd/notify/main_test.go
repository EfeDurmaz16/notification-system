package main

import (
	"context"
	"testing"
)

func TestRunRequiresExplicitModeAndDatabase(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	for _, args := range [][]string{nil, {"http"}, {"api", "worker"}, {"api"}, {"worker"}} {
		if err := run(context.Background(), args); err == nil {
			t.Fatalf("run(%q) should reject invalid startup configuration", args)
		}
	}
}

func TestProviderRetryConfiguration(t *testing.T) {
	t.Setenv("PROVIDER_URL", "https://provider.example/send")
	t.Setenv("PROVIDER_IDEMPOTENT", "false")
	t.Setenv("PROVIDER_KEY_WINDOW", "24h")
	t.Setenv("RETRY_MAX_ATTEMPTS", "9")
	t.Setenv("RETRY_BASE_BACKOFF", "3s")
	t.Setenv("PROVIDER_429_NOT_ACCEPTED", "true")
	engine, err := providerEngine(nil)
	if err != nil || engine.MaxAttempts != 9 || engine.BaseBackoff.String() != "3s" || !engine.ThrottleRejects {
		t.Fatalf("retry config: %+v, %v", engine, err)
	}
	for _, tc := range []struct{ key, value string }{
		{"RETRY_MAX_ATTEMPTS", "0"}, {"RETRY_MAX_ATTEMPTS", "abc"}, {"RETRY_BASE_BACKOFF", "0s"},
		{"RETRY_BASE_BACKOFF", "61s"}, {"PROVIDER_429_NOT_ACCEPTED", "sometimes"}, {"PROVIDER_ACK_MODE", "any"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := providerEngine(nil); err == nil {
				t.Fatal("invalid retry configuration accepted")
			}
		})
	}
}

func TestReceiptModeRequiresExplicitProviderContract(t *testing.T) {
	t.Setenv("PROVIDER_URL", "https://provider.example/send")
	for _, mode := range []string{"", "assessment", "message_id"} {
		t.Setenv("PROVIDER_ACK_MODE", mode)
		engine, err := providerEngine(nil)
		if err != nil || engine.MessageIDReceipt != (mode == "message_id") {
			t.Fatalf("mode=%q engine=%+v err=%v", mode, engine, err)
		}
	}
}

func TestDefaultThrottleContractFollowsDestination(t *testing.T) {
	for _, tc := range []struct {
		endpoint, override string
		want               bool
	}{
		{"http://provider:8081/send", "", true},
		{"https://webhook.site/example", "", false},
		{"http://provider:8081/another-endpoint", "", false},
		{"http://provider:8081/send", "false", false},
		{"https://provider.example/send", "true", true},
	} {
		t.Setenv("PROVIDER_URL", tc.endpoint)
		t.Setenv("PROVIDER_429_NOT_ACCEPTED", tc.override)
		e, err := providerEngine(nil)
		if err != nil || e.ThrottleRejects != tc.want {
			t.Fatalf("endpoint=%s override=%q engine=%+v err=%v", tc.endpoint, tc.override, e, err)
		}
	}
}
