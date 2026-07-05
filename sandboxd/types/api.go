package types

import "time"

// ClaimRequest is the wire body of POST /v1/claim.
type ClaimRequest struct {
	Template   string   `json:"template"`
	Net        NetShape `json:"net,omitempty"`
	Size       Size     `json:"size,omitempty"`
	TTLSeconds int      `json:"ttl_seconds,omitempty"`
}

// ClaimResponse is the wire reply of POST /v1/claim.
type ClaimResponse struct {
	ID       string    `json:"id"`
	Token    string    `json:"token"`
	Deadline time.Time `json:"deadline"`
}

// Key resolves the requested pool key, defaulting to the hardened lane
// (net none → FC) and the smallest tier.
func (r ClaimRequest) Key() PoolKey {
	key := PoolKey{Template: r.Template, Net: r.Net, Size: r.Size}
	if key.Net == "" {
		key.Net = NetNone
	}
	if key.Size == "" {
		key.Size = SizeSmall
	}
	return key
}

// TTL converts the wire seconds to a duration; zero means server default.
func (r ClaimRequest) TTL() time.Duration {
	return time.Duration(r.TTLSeconds) * time.Second
}
