package sandbox

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
)

const (
	VolumeAttached  VolumePhase = "attached"
	VolumePending   VolumePhase = "pending"
	VolumeDetaching VolumePhase = "detaching"
)

// VolumePhase is the progress of a desired or applied volume.
type VolumePhase string

// VolumeState reports the progress of one desired or applied volume.
type VolumeState struct {
	Name  string      `json:"name"`
	State VolumePhase `json:"state"`
	Error string      `json:"error,omitempty"`
}

// SandboxVolumesResponse separates persisted intent from confirmed applied mounts.
type SandboxVolumesResponse struct {
	VolumesWant []Volume      `json:"volumes_want"`
	Volumes     []Volume      `json:"volumes"`
	States      []VolumeState `json:"states"`
	Converging  bool          `json:"converging"`
	Error       string        `json:"error,omitempty"`
}

// VolumePatch adds and removes volumes by name; a nil slice changes nothing.
type VolumePatch struct {
	Add    []Volume `json:"add,omitzero"`
	Remove []string `json:"remove,omitzero"`
}

// VolumeError reports a failed volume request; Response is nil unless the reply carried the full view.
type VolumeError struct {
	*APIError
	Response *SandboxVolumesResponse
}

func (e *VolumeError) Unwrap() error { return e.APIError }

// GetVolumes reads the owning node's view and updates public fields; callers must serialize volume methods.
func (s *Sandbox) GetVolumes(ctx context.Context) (SandboxVolumesResponse, error) {
	return s.requestVolumes(ctx, http.MethodGet, nil, "get volumes")
}

// SetVolumes converges the full desired set; an explicit empty slice detaches all volumes, and nil is rejected.
func (s *Sandbox) SetVolumes(ctx context.Context, volumes []Volume) (SandboxVolumesResponse, error) {
	if volumes == nil {
		return SandboxVolumesResponse{}, errors.New("set volumes: use an explicit empty slice to clear volumes")
	}
	return s.requestVolumes(ctx, http.MethodPut, struct {
		Volumes []Volume `json:"volumes"`
	}{Volumes: volumes}, "set volumes")
}

// PatchVolumes adds or removes volumes by name.
func (s *Sandbox) PatchVolumes(ctx context.Context, patch VolumePatch) (SandboxVolumesResponse, error) {
	return s.requestVolumes(ctx, http.MethodPatch, patch, "patch volumes")
}

func (s *Sandbox) requestVolumes(ctx context.Context, method string, request any, verb string) (SandboxVolumesResponse, error) {
	var out SandboxVolumesResponse
	var body io.Reader
	if request != nil {
		encoded, err := encodeBody(verb, request)
		if err != nil {
			return out, err
		}
		body = bytes.NewReader(encoded)
	}
	resp, err := s.c.roundTrip(ctx, method, s.owner, "/v1/sandboxes/"+s.ID+"/volumes", body, s.c.apiToken)
	if err != nil {
		return out, err
	}
	defer func() { _ = resp.Body.Close() }()
	var wire struct {
		VolumesWant *[]Volume      `json:"volumes_want"`
		Volumes     *[]Volume      `json:"volumes"`
		States      *[]VolumeState `json:"states"`
		Converging  *bool          `json:"converging"`
		Error       string         `json:"error"`
	}
	decodeErr := json.UnmarshalRead(resp.Body, &wire)
	complete := decodeErr == nil && wire.VolumesWant != nil && wire.Volumes != nil && wire.States != nil && wire.Converging != nil
	if complete {
		out = SandboxVolumesResponse{VolumesWant: *wire.VolumesWant, Volumes: *wire.Volumes, States: *wire.States, Converging: *wire.Converging, Error: wire.Error}
		s.VolumesWant = slices.Clone(out.VolumesWant)
		s.Volumes = slices.Clone(out.Volumes)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		failure := &VolumeError{APIError: &APIError{Verb: verb, Status: resp.StatusCode, Message: wire.Error}}
		if complete {
			view := out
			view.VolumesWant, view.Volumes, view.States = slices.Clone(out.VolumesWant), slices.Clone(out.Volumes), slices.Clone(out.States)
			failure.Response = &view
		}
		return out, failure
	}
	if decodeErr != nil {
		return out, fmt.Errorf("decode %s response: %w", verb, decodeErr)
	}
	if !complete {
		return out, fmt.Errorf("decode %s response: missing volume view", verb)
	}
	return out, nil
}
