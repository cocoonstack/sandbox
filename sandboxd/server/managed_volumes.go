package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"regexp"
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

type managedVolumeGrower interface {
	GrowVolume(context.Context, string, string, types.VolumeGrow, string) (types.ManagedVolumeInfo, error)
}

var volumeOperationID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

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

func (s *Server) handleGrowVolume(w http.ResponseWriter, r *http.Request) {
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
	var body struct {
		SizeGiB            *int64  `json:"size_gib"`
		ExpectedGeneration *uint64 `json:"expected_generation"`
		OperationID        *string `json:"operation_id"`
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil || utils.DecodeStrictJSON(raw, &body) != nil || body.SizeGiB == nil || body.ExpectedGeneration == nil || body.OperationID == nil {
		volumeHTTPError(w, 400, "invalid_volume_grow", "size_gib, expected_generation and operation_id are required")
		return
	}
	if *body.SizeGiB < 1 || *body.SizeGiB > 1024 || !volumeOperationID.MatchString(*body.OperationID) || *body.OperationID == "00000000-0000-0000-0000-000000000000" {
		volumeHTTPError(w, 400, "invalid_volume_grow", "size_gib must be between 1 and 1024 and operation_id must be a canonical nonzero UUID")
		return
	}
	grower, ok := mgr.(managedVolumeGrower)
	if !ok {
		if _, err := mgr.GetVolume(r.PathValue("name"), tenantFrom(r.Context())); err != nil {
			if !writePoolErr(w, err) {
				volumeHTTPError(w, 503, "volume_storage_unavailable", "volume storage unavailable; retry")
			}
			return
		}
		volumeHTTPError(w, 503, "volume_grow_unsupported", "volume growth is not supported by this runtime")
		return
	}
	v, err := grower.GrowVolume(r.Context(), r.PathValue("name"), expected[1:len(expected)-1], types.VolumeGrow{
		SizeGiB: *body.SizeGiB, ExpectedGeneration: *body.ExpectedGeneration, OperationID: *body.OperationID,
	}, tenantFrom(r.Context()))
	if err != nil {
		if !writePoolErr(w, err) {
			volumeHTTPError(w, 503, "volume_storage_unavailable", "volume storage unavailable; retry")
		}
		return
	}
	if v.Growth == nil {
		volumeHTTPError(w, 503, "volume_storage_unavailable", "volume growth result unavailable; query the volume before retrying")
		return
	}
	status := http.StatusAccepted
	switch v.Growth.State {
	case "completed":
		status = http.StatusOK
	case "pending":
	case "recovery_required":
		volumeHTTPError(w, 409, "volume_grow_recovery_required", "volume growth requires operator recovery; inspect the volume")
		return
	default:
		volumeHTTPError(w, 503, "volume_storage_unavailable", "volume growth result unavailable; query the volume before retrying")
		return
	}
	w.Header().Set("ETag", `"`+v.ID+`"`)
	writeJSON(w, status, VolumeResponse{Volume: v})
}
