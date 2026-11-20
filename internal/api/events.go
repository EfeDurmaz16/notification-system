package api

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

func (s server) events(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	n, err := s.repo.Get(r.Context(), id)
	if err != nil {
		repositoryError(w, err)
		return
	}
	upgrader := websocket.Upgrader{HandshakeTimeout: 5 * time.Second}
	headers := http.Header{}
	headers.Set("X-Correlation-ID", w.Header().Get("X-Correlation-ID"))
	c, err := upgrader.Upgrade(w, r, headers)
	if err != nil {
		return
	}
	defer c.Close()
	done := make(chan struct{})
	c.SetReadLimit(256)
	_ = c.SetReadDeadline(time.Now().Add(40 * time.Second))
	c.SetPongHandler(func(string) error { return c.SetReadDeadline(time.Now().Add(40 * time.Second)) })
	go func() {
		defer close(done)
		// NextReader consumes protocol control frames; a data frame is not
		// part of this read-only subscription and terminates the session.
		_, _, _ = c.NextReader()
	}()
	poll := time.NewTicker(250 * time.Millisecond)
	defer poll.Stop()
	keepAlive := time.NewTicker(15 * time.Second)
	defer keepAlive.Stop()
	lastStatus := ""
	lastUpdate := time.Time{}
	for {
		if n.Status != lastStatus || !n.UpdatedAt.Equal(lastUpdate) {
			if err := c.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
				return
			}
			if err := c.WriteJSON(n); err != nil {
				return
			}
			lastStatus, lastUpdate = n.Status, n.UpdatedAt
			switch n.Status {
			case "accepted", "failed", "unknown", "cancelled":
				_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "terminal status"), time.Now().Add(time.Second))
				return
			}
		}
		select {
		case <-r.Context().Done():
			return
		case <-done:
			return
		case <-keepAlive.C:
			if err := c.WriteControl(websocket.PingMessage, nil, time.Now().Add(3*time.Second)); err != nil {
				return
			}
		case <-poll.C:
			n, err = s.repo.Get(r.Context(), id)
			if err != nil {
				_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "status unavailable"), time.Now().Add(time.Second))
				return
			}
		}
	}
}
