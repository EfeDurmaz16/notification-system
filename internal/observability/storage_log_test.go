package observability

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestStorageErrorOnlyLogsSafeFields(t *testing.T) {
	for _, tt := range []struct {
		err            error
		category, code string
	}{
		{fmt.Errorf("secret: %w", &pgconn.PgError{Code: "53300", Message: "secret"}), "postgres", "53300"},
		{&pgconn.PgError{Code: "secret", Message: "secret"}, "postgres", ""},
		{context.Canceled, "cancelled", ""},
		{context.DeadlineExceeded, "timeout", ""},
		{&net.DNSError{Err: "secret", Name: "secret"}, "network", ""},
		{&net.DNSError{IsTimeout: true}, "timeout", ""},
		{driver.ErrBadConn, "connection_lost", ""},
		{io.ErrUnexpectedEOF, "connection_lost", ""},
		{errors.New("secret"), "unclassified", ""},
	} {
		var out bytes.Buffer
		slog.New(slog.NewJSONHandler(&out, nil)).Error("failed", StorageError(tt.err))
		got := out.String()
		if !strings.Contains(got, `"category":"`+tt.category+`"`) || strings.Contains(got, "secret") ||
			(strings.Contains(got, `"sqlstate"`) != (tt.code != "")) ||
			(tt.code != "" && !strings.Contains(got, `"sqlstate":"`+tt.code+`"`)) {
			t.Fatalf("unexpected storage detail for %T: %s", tt.err, got)
		}
	}
}
