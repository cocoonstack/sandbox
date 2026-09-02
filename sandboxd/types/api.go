package types

import (
	"time"
)

// TTLField is the shared requested-lease field; zero means the server default.
type TTLField struct {
	TTLSeconds int `json:"ttl_seconds,omitempty"`
}

// TTL converts the wire seconds to a duration.
func (f TTLField) TTL() time.Duration {
	return time.Duration(f.TTLSeconds) * time.Second
}

// ClaimRequest is the wire body of POST /v1/claim.
type ClaimRequest struct {
	Template string   `json:"template"`
	Net      NetShape `json:"net,omitempty"`
	Size     Size     `json:"size,omitempty"`
	Volumes  []Volume `json:"volumes,omitempty"`
	// VolumesAttachOnly attaches every requested volume without mounting it.
	VolumesAttachOnly bool `json:"volumes_attach_only,omitempty"`
	TTLField
	NoRedirect bool `json:"no_redirect,omitempty"`
	// RequirePromoted makes the target refuse a cold-image fallback.
	RequirePromoted bool `json:"require_promoted,omitempty"`
	// ClaimRef is an opaque caller reference recorded on the claim.
	ClaimRef string `json:"claim_ref,omitempty"`
}

// Key resolves the requested pool key with the wire defaults filled.
func (r ClaimRequest) Key() PoolKey {
	return PoolKey{Template: r.Template, Net: r.Net, Size: r.Size}.Defaulted()
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
	FromCheckpoint string   `json:"from_checkpoint,omitempty"`
	Volumes        []Volume `json:"volumes,omitempty"`

	Redirect []string `json:"redirect,omitempty"`
	// RequirePromoted tells a redirecting client to preserve that requirement on retry.
	RequirePromoted bool `json:"require_promoted,omitempty"`
}

// VolumeInfo is the caller-visible, host-path-free catalog projection.
type VolumeInfo struct {
	Name         string `json:"name"`
	DefaultMount string `json:"default_mount"`
	SizeBytes    int64  `json:"size_bytes"`
	Available    bool   `json:"available"`
	Nodes        int    `json:"nodes"`
	// Writable reports whether the operator allows rw claims of this name.
	Writable bool `json:"writable,omitempty"`
}

// VolumeListResponse is the wire reply of GET /v1/volumes.
type VolumeListResponse struct {
	Volumes []VolumeInfo `json:"volumes"`
}

// ForkRequest is the wire body of POST /v1/sandboxes/{id}/fork.
type ForkRequest struct {
	Token string `json:"token"`
	Count int    `json:"count"`
	TTLField
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
	Checkpoint Checkpoint `json:"checkpoint"`
}

// CheckpointClaimRequest is the wire body of POST /v1/checkpoints/{id}/claim.
type CheckpointClaimRequest struct {
	TTLField
	// NoRedirect makes the retry resolve locally instead of bouncing between two nodes.
	NoRedirect bool `json:"no_redirect,omitempty"`
}

// CheckpointListResponse is the wire reply of GET /v1/checkpoints.
type CheckpointListResponse struct {
	Checkpoints []Checkpoint `json:"checkpoints"`
}

// PromoteRequest is the wire body of POST /v1/sandboxes/{id}/promote.
type PromoteRequest struct {
	Token    string `json:"token"`
	Template string `json:"template"`
}

// PromoteResponse returns the template's full key and stable identity.
type PromoteResponse struct {
	Key           PoolKey `json:"key"`
	ContentDigest string  `json:"content_digest"`
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
