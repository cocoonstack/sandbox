package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

type managedVolumeManager interface {
	CreateVolume(context.Context, string, types.VolumeCreate, string) (types.ManagedVolumeInfo, bool, error)
	GetVolume(string, string) (types.ManagedVolumeInfo, error)
	DeleteVolume(context.Context, string, string, string) error
	ManagedVolumeStatus() error
	ManagedVolumesEnabled() bool
}

func volumeHTTPError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, &pool.VolumeError{Status: status, Code: code, Message: message})
}

func (s *Server) requireVolumeListToken(next http.HandlerFunc) http.HandlerFunc {
	if mgr, ok := s.mgr.(managedVolumeManager); ok && mgr.ManagedVolumesEnabled() {
		return s.requireVolumeManager(next)
	}
	return s.requireToken(next)
}

func (s *Server) requireVolumeManager(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r)
		if !ok {
			volumeHTTPError(w, 401, "volume_unauthorized", "explicit root or tenant bearer token required")
			return
		}
		if s.isRootToken(token) {
			next(w, r.WithContext(withTenant(r.Context(), nil)))
			return
		}
		tenant, err := s.mgr.TenantByToken(r.Context(), token)
		if errors.Is(err, pool.ErrTenantStoreDown) {
			w.Header().Set("Retry-After", retryAfterSeconds)
			volumeHTTPError(w, 503, "tenant_store_unavailable", "tenant store unavailable; retry")
			return
		}
		if err != nil || tenant == nil {
			volumeHTTPError(w, 401, "volume_unauthorized", "explicit root or tenant bearer token required")
			return
		}
		next(w, r.WithContext(withTenant(r.Context(), tenant)))
	}
}

func (s *Server) volumeManager(w http.ResponseWriter) (managedVolumeManager, bool) {
	mgr, ok := s.mgr.(managedVolumeManager)
	if !ok || !mgr.ManagedVolumesEnabled() {
		volumeHTTPError(w, 503, "volume_storage_unavailable", "managed volume storage is not enabled")
		return nil, false
	}
	return mgr, true
}

func (s *Server) handleCreateVolume(w http.ResponseWriter, r *http.Request) {
	mgr, ok := s.volumeManager(w)
	if !ok {
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	var req types.VolumeCreate
	if err != nil || utils.DecodeStrictJSON(raw, &req) != nil {
		volumeHTTPError(w, 400, "invalid_volume", "invalid request body")
		return
	}
	v, created, err := mgr.CreateVolume(r.Context(), r.PathValue("name"), req, tenantFrom(r.Context()))
	if err != nil {
		if !writePoolErr(w, err) {
			volumeHTTPError(w, 503, "volume_storage_unavailable", "volume storage unavailable; retry")
		}
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	w.Header().Set("ETag", `"`+v.ID+`"`)
	writeJSON(w, status, VolumeResponse{Volume: v})
}

func (s *Server) handleGetVolume(w http.ResponseWriter, r *http.Request) {
	mgr, ok := s.volumeManager(w)
	if !ok {
		return
	}
	v, err := mgr.GetVolume(r.PathValue("name"), tenantFrom(r.Context()))
	if err != nil {
		writePoolErr(w, err)
		return
	}
	w.Header().Set("ETag", `"`+v.ID+`"`)
	writeJSON(w, http.StatusOK, VolumeResponse{Volume: v})
}

func (s *Server) handleDeleteVolume(w http.ResponseWriter, r *http.Request) {
	mgr, ok := s.volumeManager(w)
	if !ok {
		return
	}
	expected := r.Header.Get("If-Match")
	if expected == "" {
		volumeHTTPError(w, 428, "volume_precondition_required", "If-Match must identify the volume")
		return
	}
	if len(expected) < 3 || expected[0] != '"' || expected[len(expected)-1] != '"' || strings.ContainsAny(expected[1:len(expected)-1], "\",\\") {
		volumeHTTPError(w, 412, "volume_precondition_failed", "If-Match must contain one quoted volume identity")
		return
	}
	if err := mgr.DeleteVolume(r.Context(), r.PathValue("name"), expected[1:len(expected)-1], tenantFrom(r.Context())); err != nil {
		if !writePoolErr(w, err) {
			volumeHTTPError(w, 503, "volume_storage_unavailable", "volume storage unavailable; retry")
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
