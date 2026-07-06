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
	resp, err := ck.c.roundTrip(ctx, http.MethodPost, ck.addr, "/v1/checkpoints/"+ck.ID+"/claim", bytes.NewReader(body), ck.c.apiToken)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError("claim checkpoint", resp)
	}
	var cr claimResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return nil, fmt.Errorf("decode checkpoint claim: %w", err)
	}
	return ck.c.handleFrom(ck.addr, cr), nil
}

// Delete removes the checkpoint from its node.
func (ck *Checkpoint) Delete(ctx context.Context) error {
	resp, err := ck.c.roundTrip(ctx, http.MethodDelete, ck.addr, "/v1/checkpoints/"+ck.ID, nil, ck.c.apiToken)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return apiError("delete checkpoint", resp)
	}
	return nil
}

// Checkpoint captures the sandbox's full state — memory, disk, running
// processes — without stopping it, and returns a handle that branches new
// sandboxes from that exact moment. name is an optional label.
func (s *Sandbox) Checkpoint(ctx context.Context, name string) (*Checkpoint, error) {
	body, err := json.Marshal(checkpointRequest{Token: s.token, Name: name})
	if err != nil {
		return nil, fmt.Errorf("encode checkpoint: %w", err)
	}
	resp, err := s.postAsClaimer(ctx, "checkpoint", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError("checkpoint", resp)
	}
	var cr checkpointResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return nil, fmt.Errorf("decode checkpoint response: %w", err)
	}
	return checkpointHandle(s.c, s.owner, cr.Checkpoint), nil
}

// Checkpoints lists the CONNECTED node's checkpoints, newest first; the
// returned handles are bound to that node.
func (c *Client) Checkpoints(ctx context.Context) ([]*Checkpoint, error) {
	resp, err := c.roundTrip(ctx, http.MethodGet, c.addr, "/v1/checkpoints", nil, c.apiToken)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError("list checkpoints", resp)
	}
	var lr checkpointListResponse
	if err := json.NewDecoder(resp.Body).Decode(&lr); err != nil {
		return nil, fmt.Errorf("decode checkpoint list: %w", err)
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
