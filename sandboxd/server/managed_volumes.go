package server

import (
	"cmp"
	"io"
	"net/http"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

func volumeHTTPError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, &pool.VolumeError{Status: status, Code: code, Message: message})
}

func volumeStorageUnavailable(w http.ResponseWriter) {
	w.Header().Set("Retry-After", retryAfterSeconds)
	volumeHTTPError(w, 503, "volume_storage_unavailable", "volume storage unavailable; retry")
}

func (s *Server) handleCreateVolume(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	var req types.VolumeCreate
	if err != nil || utils.DecodeStrictJSON(raw, &req) != nil {
		volumeHTTPError(w, 400, "invalid_volume", "invalid request body")
		return
	}
	name, tenant := r.PathValue("name"), tenantFrom(r.Context())
	if owner := cmp.Or(tenant, req.Tenant); s.placer != nil && owner != "" {
		// gossip lag leaves a short window in which two nodes can both accept the name
		if peers := s.placer.VolumeOwners([]string{types.ManagedVolumeGossipName(owner, name)}); len(peers) > 0 {
			volumeHTTPError(w, 409, "volume_name_conflict", "volume exists on another node: "+strings.Join(s.clientAddrs(peers), ", "))
			return
		}
	}
	v, created, err := s.mgr.CreateVolume(r.Context(), name, req, tenant)
	if err != nil {
		if !writePoolErr(w, err) {
			volumeStorageUnavailable(w)
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
	v, err := s.mgr.GetVolume(r.PathValue("name"), tenantFrom(r.Context()))
	if err != nil {
		writePoolErr(w, err)
		return
	}
	w.Header().Set("ETag", `"`+v.ID+`"`)
	writeJSON(w, http.StatusOK, VolumeResponse{Volume: v})
}

func (s *Server) handleDeleteVolume(w http.ResponseWriter, r *http.Request) {
	id, ok := volumeIfMatch(w, r)
	if !ok {
		return
	}
	if err := s.mgr.DeleteVolume(r.Context(), r.PathValue("name"), id, tenantFrom(r.Context())); err != nil {
		if !writePoolErr(w, err) {
			volumeStorageUnavailable(w)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGrowVolume(w http.ResponseWriter, r *http.Request) {
	id, ok := volumeIfMatch(w, r)
	if !ok {
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	var req types.VolumeGrow
	if err != nil || utils.DecodeStrictJSON(raw, &req) != nil {
		volumeHTTPError(w, 400, "invalid_volume_grow", "invalid request body")
		return
	}
	v, err := s.mgr.GrowVolume(r.Context(), r.PathValue("name"), id, req.SizeGiB, tenantFrom(r.Context()))
	if err != nil {
		if !writePoolErr(w, err) {
			volumeStorageUnavailable(w)
		}
		return
	}
	w.Header().Set("ETag", `"`+v.ID+`"`)
	writeJSON(w, http.StatusOK, VolumeResponse{Volume: v})
}

// volumeIfMatch returns the one quoted volume ID in If-Match, answering 428 or 412 itself.
func volumeIfMatch(w http.ResponseWriter, r *http.Request) (string, bool) {
	expected := r.Header.Get("If-Match")
	if expected == "" {
		volumeHTTPError(w, 428, "volume_precondition_required", "If-Match must identify the volume")
		return "", false
	}
	if len(expected) < 3 || expected[0] != '"' || expected[len(expected)-1] != '"' || strings.ContainsAny(expected[1:len(expected)-1], "\",\\") {
		volumeHTTPError(w, 412, "volume_precondition_failed", "If-Match must contain one quoted volume identity")
		return "", false
	}
	return expected[1 : len(expected)-1], true
}
