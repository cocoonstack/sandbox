package server

import (
	"cmp"
	"context"
	"errors"
	"net/http"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func (s *Server) handleSandboxVolumes(w http.ResponseWriter, r *http.Request) {
	id, tenant := r.PathValue("id"), tenantFrom(r.Context())
	var view types.SandboxVolumesResponse
	var err error
	switch r.Method {
	case http.MethodGet:
		view, err = s.mgr.GetVolumes(id, tenant)
	case http.MethodPut:
		req, ok := decodeBodyStrict[types.SetVolumesRequest](w, r)
		if !ok {
			return
		}
		if req.Volumes == nil {
			writeErr(w, http.StatusBadRequest, "volumes must be an array")
			return
		}
		view, err = s.mgr.SetVolumes(r.Context(), id, tenant, req.Volumes)
	case http.MethodPatch:
		req, ok := decodeBodyStrict[types.VolumePatch](w, r)
		if !ok {
			return
		}
		view, err = s.mgr.PatchVolumes(r.Context(), id, tenant, req)
	}
	code := http.StatusOK
	if err != nil {
		message := view.Error
		code, view.Error = http.StatusInternalServerError, "volume convergence failed; inspect the volume states before retrying"
		for _, rule := range poolErrHTTP {
			if errors.Is(err, rule.err) {
				code, view.Error = rule.code, cmp.Or(rule.msg, err.Error())
				break
			}
		}
		var conflict *engine.VolumeMountConflict
		var unsupported *engine.LiveVolumeUnsupported
		switch {
		case errors.As(err, &conflict):
			code, view.Error = http.StatusConflict, conflict.Error()
		case errors.As(err, &unsupported):
			code, view.Error = http.StatusServiceUnavailable, unsupported.Error()
		case errors.Is(err, context.DeadlineExceeded):
			code, view.Error = http.StatusGatewayTimeout, "volume change timed out; inspect the volume states before retrying"
		case errors.Is(err, context.Canceled):
			code, view.Error = http.StatusRequestTimeout, "volume change canceled; inspect the volume states before retrying"
		}
		view.Error = cmp.Or(message, view.Error)
		if code >= http.StatusInternalServerError {
			log.WithFunc("server.handleSandboxVolumes").Errorf(r.Context(), err, "converge volumes for %s", id)
		}
	}
	writeJSON(w, code, view)
}
