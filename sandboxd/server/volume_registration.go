package server

import (
	"errors"
	"net/http"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func (s *Server) handleVolumeRegistration(w http.ResponseWriter, r *http.Request) {
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
	if errors.Is(err, pool.ErrVolumeBusy) {
		err = pool.ErrVolumeBusy
	}
	writeResult(w, r, op, name, op+" failed", err, noContent(w))
}
