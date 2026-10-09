package server

import (
	"net/http"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func (s *Server) handleAttachVolumes(w http.ResponseWriter, r *http.Request) {
	token, ok := sandboxToken(w, r)
	if !ok {
		return
	}
	req, ok := decodeBodyStrict[types.AttachVolumesRequest](w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	volumes, err := s.mgr.AttachVolumes(r.Context(), id, s.sandboxCred(token), req.Volumes)
	writeResult(w, r, "attach volumes", id, "attach volumes failed; retry the request or detach the pending volume", err, func() {
		writeJSON(w, http.StatusOK, types.SandboxVolumesResponse{Volumes: volumes})
	})
}

func (s *Server) handleDetachVolumes(w http.ResponseWriter, r *http.Request) {
	token, ok := sandboxToken(w, r)
	if !ok {
		return
	}
	req, ok := decodeBodyStrict[types.DetachVolumesRequest](w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	volumes, err := s.mgr.DetachVolumes(r.Context(), id, s.sandboxCred(token), req.Names)
	writeResult(w, r, "detach volumes", id, "detach volumes failed; retry the request", err, func() {
		writeJSON(w, http.StatusOK, types.SandboxVolumesResponse{Volumes: volumes})
	})
}
