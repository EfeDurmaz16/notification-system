package api

import (
	"net/http"
	"net/url"
	"strconv"
)

func (s server) history(w http.ResponseWriter, r *http.Request) {
	var after int64
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid history query")
		return
	}
	if len(q) != 0 {
		values := q["after"]
		if len(q) != 1 || len(values) != 1 {
			problem(w, http.StatusBadRequest, "supply a single after parameter")
			return
		}
		position, err := strconv.ParseUint(values[0], 10, 63)
		if err != nil {
			problem(w, http.StatusBadRequest, "invalid after position")
			return
		}
		after = int64(position)
	}
	page, err := s.repo.History(r.Context(), r.PathValue("id"), after)
	if err != nil {
		repositoryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}
