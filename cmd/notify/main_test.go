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
