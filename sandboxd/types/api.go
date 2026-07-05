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
