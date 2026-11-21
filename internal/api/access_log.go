package api

import (
	"bufio"
	"net"
	"net/http"
)

// Gorilla writes the WebSocket upgrade directly to the hijacked connection.
// All other handlers report their HTTP status through WriteHeader.
type accessWriter struct {
	http.ResponseWriter
	status int
}

func (w *accessWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *accessWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *accessWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(w.ResponseWriter).Hijack()
}
