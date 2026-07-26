package types

import (
	"time"
)

// ClaimRequest is the wire body of POST /v1/claim. NoRedirect is set by the
// SDK on a claim it is retrying at a redirect target, so that node warm-or-
// provisions locally instead of bouncing the claim back on a stale view.
type ClaimRequest struct {
	Template   string   `json:"template"`
	Net        NetShape `json:"net,omitempty"`
	Size       Size     `json:"size,omitempty"`
	Engine     Engine   `json:"engine,omitempty"`
	TTLSeconds int      `json:"ttl_seconds,omitempty"`
	NoRedirect bool     `json:"no_redirect,omitempty"`
	// ClaimRef is an opaque caller reference (the aggregated apiserver passes
	// the k8s "<namespace>/<name>") recorded on the claim so the read path can
	// map a listed sandbox back to the name it was claimed under. Optional.
	ClaimRef string `json:"claim_ref,omitempty"`
}

// Key resolves the requested pool key with the wire defaults filled.
func (r ClaimRequest) Key() PoolKey {
	return PoolKey{Template: r.Template, Net: r.Net, Size: r.Size, Engine: r.Engine}.Defaulted()
}

// TTL converts the wire seconds to a duration; zero means server default.
func (r ClaimRequest) TTL() time.Duration {
	return time.Duration(r.TTLSeconds) * time.Second
}

// ClaimResponse is the wire reply of POST /v1/claim. A successful claim
// carries ID/Token/Deadline/OwnerAddr; a mesh miss carries Redirect (peer
// addresses to retry, MOVED-style), and the two are mutually exclusive.
type ClaimResponse struct {
	ID        string    `json:"id,omitempty"`
	Token     string    `json:"token,omitempty"`
	Deadline  time.Time `json:"deadline,omitzero"`
	OwnerAddr string    `json:"owner_addr,omitempty"`

	// FromCheckpoint names the checkpoint a branched claim was born from,
	// so clients can reconstruct the checkpoint tree.
	FromCheckpoint string `json:"from_checkpoint,omitempty"`

	Redirect []string `json:"redirect,omitempty"`
}

// ForkRequest is the wire body of POST /v1/sandboxes/{id}/fork. The
// Authorization header carries an api or tenant token (forking creates node
// resources, like a claim); Token proves ownership of the source sandbox.
// TTLSeconds applies to every child; zero means the server default — a
// lease is a per-sandbox resource bound, so children never inherit the
// parent's remainder.
type ForkRequest struct {
	Token      string `json:"token"`
	Count      int    `json:"count"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}

// TTL converts the wire seconds to a duration; zero means server default.
func (r ForkRequest) TTL() time.Duration {
	return time.Duration(r.TTLSeconds) * time.Second
}

// ForkResponse carries one claim per child.
type ForkResponse struct {
	Children []ClaimResponse `json:"children"`
}

// CheckpointRequest is the wire body of POST /v1/sandboxes/{id}/checkpoint.
// Auth mirrors fork: api/tenant token in Authorization, sandbox token here.
type CheckpointRequest struct {
	Token string `json:"token"`
	Name  string `json:"name,omitempty"`
}

// CheckpointResponse is its reply.
type CheckpointResponse struct {
	Checkpoint Checkpoint `json:"checkpoint"`
}

// CheckpointClaimRequest is the wire body of POST /v1/checkpoints/{id}/claim.
// TTLSeconds semantics match a claim's; zero means the server default.
type CheckpointClaimRequest struct {
	TTLSeconds int `json:"ttl_seconds,omitempty"`
	// NoRedirect is set by a client retrying at a redirect target, so the retry
	// resolves locally instead of bouncing between two nodes.
	NoRedirect bool `json:"no_redirect,omitempty"`
}

// TTL converts the requested lease.
func (r CheckpointClaimRequest) TTL() time.Duration {
	return time.Duration(r.TTLSeconds) * time.Second
}

// CheckpointListResponse is the wire reply of GET /v1/checkpoints.
type CheckpointListResponse struct {
	Checkpoints []Checkpoint `json:"checkpoints"`
}

// PromoteRequest is the wire body of POST /v1/sandboxes/{id}/promote; auth
// mirrors ForkRequest (control token in the header, ownership in Token).
type PromoteRequest struct {
	Token    string `json:"token"`
	Template string `json:"template"`
}

// PromoteResponse returns the template's full key: templates are node-local,
// so a cluster client needs the exact key (and this node's address) to claim
// from or delete the template later.
type PromoteResponse struct {
	Key PoolKey `json:"key"`
}

// PreviewRequest is the wire body of POST /v1/sandboxes/{id}/preview; auth
// mirrors ForkRequest (control token in the header, ownership in Token).
type PreviewRequest struct {
	Token      string `json:"token"`
	Port       uint16 `json:"port"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}

// PreviewResponse carries the minted URL.
type PreviewResponse struct {
	URL string `json:"url"`
}
