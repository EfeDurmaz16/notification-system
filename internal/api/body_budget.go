package api

import (
	"net/http"
	"sync"
)

// Limit the total encoded body size of concurrent creates in this API replica.
// Both endpoints share the budget. Unknown lengths reserve a full body so a
// chunked request cannot undercount its reservation.
func limitCreateBodies(next http.HandlerFunc) http.HandlerFunc {
	const budget = 4 * maxBody
	var mu sync.Mutex
	var reserved int64
	return func(w http.ResponseWriter, r *http.Request) {
		size := r.ContentLength
		if size > maxBody {
			next(w, r) // readBody returns 413 without reading or allocating.
			return
		}
		if size < 0 {
			size = maxBody
		}
		size = max(size, 16<<10)
		mu.Lock()
		if reserved+size > budget {
			mu.Unlock()
			w.Header().Set("Retry-After", "1")
			problem(w, http.StatusServiceUnavailable, "create capacity temporarily exhausted")
			return
		}
		reserved += size
		mu.Unlock()
		defer func() {
			mu.Lock()
			reserved -= size
			mu.Unlock()
		}()
		next(w, r)
	}
}
