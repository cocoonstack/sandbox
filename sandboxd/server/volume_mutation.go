package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func (s *Server) handleSandboxVolumes(w http.ResponseWriter, r *http.Request) {
	id, tenant := r.PathValue("id"), tenantFrom(r.Context())
	var (
		view types.SandboxVolumesResponse
		err  error
	)
	switch r.Method {
	case http.MethodGet:
		view, err = s.mgr.GetVolumes(id, tenant)
	case http.MethodPut:
		req, ok := decodeBodyStrict[SetVolumesRequest](w, r)
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
	if errors.Is(err, pool.ErrUnknownSandbox) {
		writePoolErr(w, err)
		return
	}
	code := http.StatusOK
	if err != nil {
		code, view.Error = http.StatusInternalServerError, "volume convergence failed; inspect the volume states before retrying"
		conflict, isConflict := errors.AsType[*pool.VolumeMountConflict](err)
		unsupported, isUnsupported := errors.AsType[*pool.LiveVolumeUnsupported](err)
		switch {
		case isConflict:
			code, view.Error = http.StatusConflict, conflict.Error()
		case isUnsupported:
			code, view.Error = http.StatusServiceUnavailable, unsupported.Error()
		case errors.Is(err, context.DeadlineExceeded):
			code, view.Error = http.StatusGatewayTimeout, "volume change timed out; inspect the volume states before retrying"
		case errors.Is(err, context.Canceled):
			code, view.Error = http.StatusRequestTimeout, "volume change canceled; inspect the volume states before retrying"
		default:
			if mapped, msg, ok := mapPoolErr(w, err); ok {
				code, view.Error = mapped, msg
			}
		}
		if code >= http.StatusInternalServerError && !errors.Is(err, pool.ErrReplicaPresent) {
			log.WithFunc("server.handleSandboxVolumes").Errorf(r.Context(), err, "converge volumes for %s", id)
		}
	}
	writeJSON(w, code, view)
}
