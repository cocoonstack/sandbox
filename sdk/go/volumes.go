package sandbox

import (
	"bytes"
	"context"
	"net/http"
	"slices"
)

// VolumeMutation describes an interrupted operation in a SandboxSummary.
// Retry the attach, detach this name, or release the sandbox to recover.
type VolumeMutation struct {
	Volume Volume `json:"volume"`
	Detach bool   `json:"detach,omitzero"`
}

// AttachVolumes adds read-only catalog mounts to this running sandbox.
// Retry the same request after a partial failure, or detach the pending name.
// On success it replaces Volumes and returns a detached copy of the full set.
// Like Renew, callers must serialize updates to this handle's public fields.
func (s *Sandbox) AttachVolumes(ctx context.Context, volumes ...Volume) ([]Volume, error) {
	return s.mutateVolumes(ctx, "attach", struct {
		Volumes []Volume `json:"volumes"`
	}{Volumes: volumes})
}

// DetachVolumes removes read-only volumes, including a pending attach.
// Already absent names are harmless. Batches may partially complete on error;
// retry the request to finish it. It replaces Volumes only on success.
func (s *Sandbox) DetachVolumes(ctx context.Context, names ...string) ([]Volume, error) {
	return s.mutateVolumes(ctx, "detach", struct {
		Names []string `json:"names"`
	}{Names: names})
}

func (s *Sandbox) mutateVolumes(ctx context.Context, verb string, request any) ([]Volume, error) {
	body, err := encodeBody(verb+" volumes", request)
	if err != nil {
		return nil, err
	}
	response, err := s.c.doJSON[struct {
		Volumes []Volume `json:"volumes"`
	}](ctx, http.MethodPost, s.owner, "/v1/sandboxes/"+s.ID+"/volumes/"+verb, bytes.NewReader(body), s.token, verb+" volumes")
	if err != nil {
		return nil, err
	}
	s.Volumes = slices.Clone(response.Volumes)
	return slices.Clone(s.Volumes), nil
}
