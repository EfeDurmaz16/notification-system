package api

import (
	"encoding/base64"
	"strings"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/store"
)

func encodeListCursor(n store.Notification) string {
	position := n.CreatedAt.UTC().Format(time.RFC3339Nano) + "/" + n.ID
	return base64.RawURLEncoding.EncodeToString([]byte(position))
}

func decodeListCursor(token string) *store.ListPosition {
	if len(token) > 100 {
		return nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil
	}
	stamp, id, ok := strings.Cut(string(raw), "/")
	if !ok || !idPattern.MatchString(id) {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil || t.UTC().Year() < 1 || t.UTC().Year() > 9999 {
		return nil
	}
	return &store.ListPosition{CreatedAt: t, ID: id}
}
