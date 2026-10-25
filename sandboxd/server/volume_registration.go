package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func (s *Server) handleVolumeRegistration(w http.ResponseWriter, r *http.Request) {
	if s.apiToken == "" {
		writeErr(w, http.StatusForbidden, "volume registration requires a configured api token")
		return
	}
	name := r.PathValue("name")
	if !types.ValidVolumeName(name) {
		writeErr(w, http.StatusBadRequest, "invalid volume name")
		return
	}
	var err error
	op := "unregister volume"
	if r.Method == http.MethodPut {
		req, ok := decodeBodyStrict[types.VolumeRegistration](w, r)
		if !ok {
			return
		}
		op = "register volume"
		err = s.mgr.RegisterVolume(r.Context(), name, req)
	} else {
		err = s.mgr.UnregisterVolume(r.Context(), name)
	}
	switch {
	case errors.Is(err, pool.ErrVolumeCatalogUnavailable):
		writePoolErr(w, err)
	case errors.Is(err, context.DeadlineExceeded):
		writeErr(w, http.StatusGatewayTimeout, op+" timed out")
	case errors.Is(err, context.Canceled):
		writeErr(w, http.StatusRequestTimeout, op+" canceled")
	default:
		writeResult(w, r, op, name, op+" failed", err, noContent(w))
	}
}
