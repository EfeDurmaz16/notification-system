package api

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestHealthAccessLogLevels(t *testing.T) {
	var out bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	repo := new(repositoryStub)
	handler := New(repo)
	for _, tt := range []struct {
		path  string
		err   error
		level string
	}{
		{"/healthz", nil, "DEBUG"},
		{"/healthz", errors.New("unavailable"), "WARN"},
		{"/v1/notifications", nil, "INFO"},
	} {
		out.Reset()
		repo.err = tt.err
		call(handler, "GET", tt.path, "", "")
		if !strings.Contains(out.String(), `"level":"`+tt.level+`"`) {
			t.Fatalf("unexpected access log for %s: %s", tt.path, out.String())
		}
	}
}
