package observability

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"net"
	"regexp"
)

var sqlState = regexp.MustCompile(`^[0-9A-Z]{5}$`)

// StorageError exposes the category and PostgreSQL's stable five-character
// code. Driver messages can contain connection strings, SQL or row values.
func StorageError(err error) slog.Attr {
	category := "unclassified"
	var pg interface{ SQLState() string }
	var network net.Error
	switch {
	case errors.Is(err, context.Canceled):
		category = "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		category = "timeout"
	case errors.As(err, &pg):
		category = "postgres"
		if code := pg.SQLState(); sqlState.MatchString(code) {
			return slog.Group("storage", "category", category, "sqlstate", code)
		}
	case errors.As(err, &network):
		category = "network"
		if network.Timeout() {
			category = "timeout"
		}
	case errors.Is(err, driver.ErrBadConn), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		category = "connection_lost"
	}
	return slog.Group("storage", "category", category)
}
