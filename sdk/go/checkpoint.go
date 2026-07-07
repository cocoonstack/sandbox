package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Checkpoint is a captured sandbox state bound to the node that holds it.
// New claims fresh sandboxes branched from the captured moment, any number
// of times; the source sandbox is unaffected and can keep being
// checkpointed, so successive captures form a tree.
type Checkpoint struct {
	ID        string
	Name      string
	SandboxID string
	CreatedAt time.Time

	c    *Client
	addr string
}

// New claims a sandbox cloned from the checkpoint, on the checkpoint's
// node. The snapshot pins the key axes; WithTimeout may set the TTL.
func (ck *Checkpoint) New(ctx context.Context, opts ...Option) (*Sandbox, error) {
	var claim claimRequest
	for _, opt := range opts {
		opt(&claim)
	}
	body, err := json.Marshal(checkpointClaimRequest{TTLSeconds: claim.TTLSeconds})
	if err != nil {
		return nil, fmt.Errorf("encode checkpoint claim: %w", err)
	}
	cr, err := doJSON[claimResponse](ctx, ck.c, http.MethodPost, ck.addr, "/v1/checkpoints/"+ck.ID+"/claim", bytes.NewReader(body), ck.c.apiToken, "claim checkpoint")
	if err != nil {
		return nil, err
	}
	return ck.c.handleFrom(ck.addr, cr), nil
}

// Delete removes the checkpoint from its node.
func (ck *Checkpoint) Delete(ctx context.Context) error {
	return doNoContent(ctx, ck.c, http.MethodDelete, ck.addr, "/v1/checkpoints/"+ck.ID, nil, ck.c.apiToken, "delete checkpoint")
}

// Checkpoint captures the sandbox's full state — memory, disk, running
// processes — without stopping it, and returns a handle that branches new
// sandboxes from that exact moment. name is an optional label.
func (s *Sandbox) Checkpoint(ctx context.Context, name string) (*Checkpoint, error) {
	body, err := json.Marshal(checkpointRequest{Token: s.token, Name: name})
	if err != nil {
		return nil, fmt.Errorf("encode checkpoint: %w", err)
	}
	cr, err := doJSON[checkpointResponse](ctx, s.c, http.MethodPost, s.owner, "/v1/sandboxes/"+s.ID+"/checkpoint", bytes.NewReader(body), s.c.apiToken, "checkpoint")
	if err != nil {
		return nil, err
	}
	return checkpointHandle(s.c, s.owner, cr.Checkpoint), nil
}

// Checkpoints lists the CONNECTED node's checkpoints, newest first; the
// returned handles are bound to that node.
func (c *Client) Checkpoints(ctx context.Context) ([]*Checkpoint, error) {
	lr, err := doJSON[checkpointListResponse](ctx, c, http.MethodGet, c.addr, "/v1/checkpoints", nil, c.apiToken, "list checkpoints")
	if err != nil {
		return nil, err
	}
	ckpts := make([]*Checkpoint, len(lr.Checkpoints))
	for i, rec := range lr.Checkpoints {
		ckpts[i] = checkpointHandle(c, c.addr, rec)
	}
	return ckpts, nil
}

func checkpointHandle(c *Client, addr string, rec checkpointRecord) *Checkpoint {
	return &Checkpoint{
		ID: rec.ID, Name: rec.Name, SandboxID: rec.SandboxID, CreatedAt: rec.CreatedAt,
		c: c, addr: addr,
	}
}

type checkpointRequest struct {
	Token string `json:"token"`
	Name  string `json:"name,omitempty"`
}

type checkpointRecord struct {
	ID        string    `json:"id"`
	Name      string    `json:"name,omitempty"`
	SandboxID string    `json:"sandbox_id"`
	CreatedAt time.Time `json:"created_at"`
}

type checkpointResponse struct {
	Checkpoint checkpointRecord `json:"checkpoint"`
}

type checkpointClaimRequest struct {
	TTLSeconds int `json:"ttl_seconds,omitempty"`
}

type checkpointListResponse struct {
	Checkpoints []checkpointRecord `json:"checkpoints"`
}
