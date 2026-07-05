package types

import (
	"cmp"
	"time"
)

// ClaimRequest is the wire body of POST /v1/claim.
type ClaimRequest struct {
	Template   string   `json:"template"`
	Net        NetShape `json:"net,omitempty"`
	Size       Size     `json:"size,omitempty"`
	TTLSeconds int      `json:"ttl_seconds,omitempty"`
}

// ClaimResponse is the wire reply of POST /v1/claim. OwnerAddr is the node
// that owns the sandbox; on a single node it is the node's own address, and at
// M2c it is the redirect target the SDK dials for the data plane.
type ClaimResponse struct {
	ID        string    `json:"id"`
	Token     string    `json:"token"`
	Deadline  time.Time `json:"deadline"`
	OwnerAddr string    `json:"owner_addr,omitempty"`
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
