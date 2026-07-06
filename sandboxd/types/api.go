package types

import (
	"cmp"
	"time"
)

// ClaimRequest is the wire body of POST /v1/claim. NoRedirect is set by the
// SDK on a claim it is retrying at a redirect target, so that node warm-or-
// provisions locally instead of bouncing the claim back on a stale view.
type ClaimRequest struct {
	Template   string   `json:"template"`
	Net        NetShape `json:"net,omitempty"`
	Size       Size     `json:"size,omitempty"`
	TTLSeconds int      `json:"ttl_seconds,omitempty"`
	NoRedirect bool     `json:"no_redirect,omitempty"`
}

// ClaimResponse is the wire reply of POST /v1/claim. A successful claim
// carries ID/Token/Deadline/OwnerAddr; a mesh miss carries Redirect (peer
// addresses to retry, MOVED-style), and the two are mutually exclusive.
type ClaimResponse struct {
	ID        string    `json:"id,omitempty"`
	Token     string    `json:"token,omitempty"`
	Deadline  time.Time `json:"deadline,omitzero"`
	OwnerAddr string    `json:"owner_addr,omitempty"`

	Redirect []string `json:"redirect,omitempty"`
}

// Key resolves the requested pool key, defaulting to the hardened lane
// (net none → FC) and the smallest tier.
func (r ClaimRequest) Key() PoolKey {
	return PoolKey{
		Template: r.Template,
		Net:      cmp.Or(r.Net, NetNone),
		Size:     cmp.Or(r.Size, SizeSmall),
	}
}

// TTL converts the wire seconds to a duration; zero means server default.
func (r ClaimRequest) TTL() time.Duration {
	return time.Duration(r.TTLSeconds) * time.Second
}

// ForkRequest is the wire body of POST /v1/sandboxes/{id}/fork. TTLSeconds
// applies to every child; zero means the server default — a lease is a
// per-sandbox resource bound, so children never inherit the parent's
// remainder.
type ForkRequest struct {
	Count      int `json:"count"`
	TTLSeconds int `json:"ttl_seconds,omitempty"`
}

// TTL converts the wire seconds to a duration; zero means server default.
func (r ForkRequest) TTL() time.Duration {
	return time.Duration(r.TTLSeconds) * time.Second
}

// ForkResponse carries one claim per child.
type ForkResponse struct {
	Children []ClaimResponse `json:"children"`
}

// PromoteRequest is the wire body of POST /v1/sandboxes/{id}/promote.
type PromoteRequest struct {
	Template string `json:"template"`
}
