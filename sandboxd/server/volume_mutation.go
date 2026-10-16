package server

import (
	"cmp"
	"errors"
	"net/http"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func (s *Server) handleSandboxVolumes(w http.ResponseWriter, r *http.Request) {
	token, ok := sandboxToken(w, r)
	if !ok {
		return
	}
	id, cred := r.PathValue("id"), s.sandboxCred(token)
	var view types.SandboxVolumesResponse
	var err error
	switch r.Method {
	case http.MethodGet:
		view, err = s.mgr.GetVolumes(id, cred)
	case http.MethodPut:
		req, ok := decodeBodyStrict[types.SetVolumesRequest](w, r)
		if !ok {
			return
		}
		if req.Volumes == nil {
			writeErr(w, http.StatusBadRequest, "volumes must be an array")
			return
		}
		view, err = s.mgr.SetVolumes(r.Context(), id, cred, req.Volumes)
	case http.MethodPatch:
		req, ok := decodeBodyStrict[types.VolumePatch](w, r)
		if !ok {
			return
		}
		view, err = s.mgr.PatchVolumes(r.Context(), id, cred, req)
	}
	code := http.StatusOK
	if err != nil {
		code, view.Error = http.StatusInternalServerError, "volume convergence failed; retry the desired set"
		for _, rule := range poolErrHTTP {
			if errors.Is(err, rule.err) {
				code, view.Error = rule.code, cmp.Or(rule.msg, err.Error())
				break
			}
		}
		if code == http.StatusInternalServerError {
			log.WithFunc("server.handleSandboxVolumes").Errorf(r.Context(), err, "converge volumes for %s", id)
		}
		for i := range view.States {
			if view.States[i].Error != "" {
				view.States[i].Error = view.Error
			}
		}
	}
	writeJSON(w, code, view)
}
