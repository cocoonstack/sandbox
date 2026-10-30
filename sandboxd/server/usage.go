package server

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
	if s.usage == nil {
		writeErr(w, http.StatusNotImplemented, "no shared usage store configured")
		return
	}
	q, err := usageQuery(r.URL.Query())
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	recs, err := s.usage.List(r.Context(), q)
	if err != nil {
		log.WithFunc("server.handleUsage").Error(r.Context(), err, "list usage")
		writeErr(w, http.StatusServiceUnavailable, "usage store down; retry")
		return
	}
	resp := UsageResponse{Events: recs}
	if len(recs) == q.Limit {
		resp.Next = recs[len(recs)-1].Cursor
	}
	writeJSON(w, http.StatusOK, resp)
}

func usageQuery(v url.Values) (usage.Query, error) {
	q := usage.Query{Tenant: v.Get("tenant")}
	var err error
	if raw := v.Get("after"); raw != "" {
		if q.After, err = strconv.ParseInt(raw, 10, 64); err != nil || q.After < 0 {
			return usage.Query{}, errors.New("after must be a non-negative cursor")
		}
	}
	if q.Limit, err = pageLimit(v.Get("limit")); err != nil {
		return usage.Query{}, err
	}
	if q.Since, err = usageTime(v, "since"); err != nil {
		return usage.Query{}, err
	}
	if q.Until, err = usageTime(v, "until"); err != nil {
		return usage.Query{}, err
	}
	return q, nil
}

func usageTime(v url.Values, name string) (time.Time, error) {
	raw := v.Get(name)
	if raw == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be an RFC 3339 time", name)
	}
	return t, nil
}
