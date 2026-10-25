package server

import (
	"errors"
	"slices"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// TTLField is the shared requested-lease field; zero means the server default.
type TTLField struct {
	TTLSeconds int `json:"ttl_seconds,omitzero"`
}

// TTL converts the wire seconds to a duration.
func (f TTLField) TTL() time.Duration {
	return types.Seconds(f.TTLSeconds)
}

// ClaimRequest is the wire body of POST /v1/claim.
type ClaimRequest struct {
	Template string         `json:"template"`
	Net      types.NetShape `json:"net,omitempty"`
	Size     types.Size     `json:"size,omitempty"`
	Volumes  []types.Volume `json:"volumes,omitempty"`
	// VolumesAttachOnly attaches every requested volume without mounting it.
	VolumesAttachOnly bool `json:"volumes_attach_only,omitzero"`
	TTLField
	NoRedirect bool `json:"no_redirect,omitzero"`
	// RequirePromoted makes the target refuse a cold-image fallback.
	RequirePromoted bool `json:"require_promoted,omitzero"`
	// ClaimRef is an opaque caller reference recorded on the claim.
	ClaimRef string             `json:"claim_ref,omitempty"`
	Metadata types.Metadata     `json:"metadata,omitempty"`
	OnExpire types.ExpireAction `json:"on_expire,omitempty"`
	// Egress false claims with no egress policy, whatever the pool's; unset keeps the policy.
	Egress *bool     `json:"egress,omitzero"`
	Env    types.Env `json:"env,omitempty"`
}

// Key resolves the requested pool key with the wire defaults filled.
func (r ClaimRequest) Key() types.PoolKey {
	return types.PoolKey{Template: r.Template, Net: r.Net, Size: r.Size}.Defaulted()
}

func (r ClaimRequest) normalizedVolumes() ([]types.Volume, error) {
	volumes := slices.Clone(r.Volumes)
	if r.VolumesAttachOnly {
		if len(volumes) == 0 {
			return nil, errors.New("volumes_attach_only requires at least one volume")
		}
		for i := range volumes {
			volumes[i].AttachOnly = true
		}
	}
	return types.ValidateVolumes(volumes)
}

// ClaimResponse is the wire reply of POST /v1/claim; success and Redirect are exclusive.
type ClaimResponse struct {
	ID        string    `json:"id,omitempty"`
	Token     string    `json:"token,omitempty"`
	Deadline  time.Time `json:"deadline,omitzero"`
	OwnerAddr string    `json:"owner_addr,omitempty"`
	// TemplateDigest is the content identity of the export this claim was cloned from.
	TemplateDigest string `json:"template_digest,omitempty"`

	// FromCheckpoint names the checkpoint a branched claim was born from.
	FromCheckpoint string         `json:"from_checkpoint,omitempty"`
	Volumes        []types.Volume `json:"volumes,omitempty"`

	Redirect []string `json:"redirect,omitempty"`
	// RequirePromoted tells a redirecting client to preserve that requirement on retry.
	RequirePromoted bool `json:"require_promoted,omitzero"`
	// NetRoute is how the guest reaches the network; a foreign guest agent sets its processes' proxy from it.
	NetRoute types.NetRoute `json:"net_route,omitempty"`
}

// VolumeListResponse is the wire reply of GET /v1/volumes.
type VolumeListResponse struct {
	Volumes []types.VolumeInfo `json:"volumes"`
}

// SetVolumesRequest is the wire body of PUT /v1/sandboxes/{id}/volumes.
type SetVolumesRequest struct {
	Volumes []types.Volume `json:"volumes"`
}

// ForkRequest is the wire body of POST /v1/sandboxes/{id}/fork.
type ForkRequest struct {
	Token string `json:"token"`
	Count int    `json:"count"`
	// ClaimRefPrefix, when set, records each child under prefix + its id.
	ClaimRefPrefix string `json:"claim_ref_prefix,omitempty"`
	TTLField
	OnExpire types.ExpireAction `json:"on_expire,omitempty"`
}

// ForkResponse carries one claim per child.
type ForkResponse struct {
	Children []ClaimResponse `json:"children"`
}

// CheckpointRequest is the wire body of POST /v1/sandboxes/{id}/checkpoint.
type CheckpointRequest struct {
	Token string `json:"token"`
	Name  string `json:"name,omitempty"`
}

// CheckpointResponse is its reply.
type CheckpointResponse struct {
	Checkpoint types.Checkpoint `json:"checkpoint"`
}

// CheckpointClaimRequest is the wire body of POST /v1/checkpoints/{id}/claim.
type CheckpointClaimRequest struct {
	TTLField
	// NoRedirect makes the retry resolve locally instead of bouncing between two nodes.
	NoRedirect bool               `json:"no_redirect,omitzero"`
	Metadata   types.Metadata     `json:"metadata,omitempty"`
	OnExpire   types.ExpireAction `json:"on_expire,omitempty"`
	Env        types.Env          `json:"env,omitempty"`
}

// CheckpointListResponse is the wire reply of GET /v1/checkpoints.
type CheckpointListResponse struct {
	Checkpoints []types.Checkpoint `json:"checkpoints"`
}

// PromoteRequest is the wire body of POST /v1/sandboxes/{id}/promote.
type PromoteRequest struct {
	Token    string `json:"token"`
	Template string `json:"template"`
}

// PromoteResponse returns the template's full key and stable identity.
type PromoteResponse struct {
	Key           types.PoolKey `json:"key"`
	ContentDigest string        `json:"content_digest"`
}

// RenewRequest is the wire body of POST /v1/sandboxes/{id}/renew.
type RenewRequest struct {
	TTLField
	OnExpire types.ExpireAction `json:"on_expire,omitempty"`
}

// RenewResponse carries the lease deadline the node granted.
type RenewResponse struct {
	Deadline time.Time `json:"deadline"`
}

// PreviewRequest is the wire body of POST /v1/sandboxes/{id}/preview.
type PreviewRequest struct {
	Token string `json:"token"`
	Port  uint16 `json:"port"`
	TTLField
}

// PreviewResponse carries the minted URL.
type PreviewResponse struct {
	URL string `json:"url"`
}
