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
