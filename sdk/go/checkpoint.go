package sandbox

import (
	"bytes"
	"context"
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
// node. The snapshot pins the key axes; WithTimeout may set the TTL. A node
// that no longer holds the checkpoint redirects to a probed owner, which New
// follows transparently; if every candidate fails transiently, New falls
// back to the origin once so it heals (pulls the checkpoint) locally.
func (ck *Checkpoint) New(ctx context.Context, opts ...Option) (*Sandbox, error) {
	var claim claimRequest
	for _, opt := range opts {
		opt(&claim)
	}
	if err := claim.rejectPinnedAxes(); err != nil {
		return nil, err
	}
	body, err := encodeBody("checkpoint claim", checkpointClaimRequest{TTLSeconds: claim.TTLSeconds})
	if err != nil {
		return nil, err
	}
	cr, err := ck.claimAt(ctx, ck.addr, body)
	if err != nil {
		return nil, err
	}
	if len(cr.Redirect) == 0 {
		return ck.c.handleFrom(ck.addr, cr), nil
	}
	body, err = encodeBody("checkpoint claim", checkpointClaimRequest{TTLSeconds: claim.TTLSeconds, NoRedirect: true})
	if err != nil {
		return nil, err
	}
	addr, target, err := redirectFallback(ck.addr, cr.Redirect, func(a string) (claimResponse, error) {
		return ck.claimAt(ctx, a, body)
	})
	if err != nil {
		return nil, fmt.Errorf("claim checkpoint: %w", err)
	}
	return ck.c.handleFrom(addr, target), nil
}

// Delete removes the checkpoint from its node and asks every peer that node
// currently sees to drop any replica a heal pulled — best-effort eventual
// cleanup, not a fleet-wide revocation. A peer that misses that broadcast
// (offline, partitioned, or joined later) keeps serving branches from its
// replica until the node's checkpoint_ttl_hours ages it out; with that TTL
// at its default of 0 (keep forever), an unreachable peer's replica has no
// cleanup bound at all. See the cluster docs' placement lifecycle.
func (ck *Checkpoint) Delete(ctx context.Context) error {
	return doNoContent(ctx, ck.c, http.MethodDelete, ck.addr, "/v1/checkpoints/"+ck.ID, nil, ck.c.apiToken, "delete checkpoint")
}

func (ck *Checkpoint) claimAt(ctx context.Context, addr string, body []byte) (claimResponse, error) {
	return doJSON[claimResponse](ctx, ck.c, http.MethodPost, addr, "/v1/checkpoints/"+ck.ID+"/claim", bytes.NewReader(body), ck.c.apiToken, "claim checkpoint")
}

// Checkpoint captures the sandbox's full state — memory, disk, running
// processes — without stopping it, and returns a handle that branches new
// sandboxes from that exact moment. name is an optional label.
func (s *Sandbox) Checkpoint(ctx context.Context, name string) (*Checkpoint, error) {
	body, err := encodeBody("checkpoint", checkpointRequest{Token: s.token, Name: name})
	if err != nil {
		return nil, err
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
	TTLSeconds int  `json:"ttl_seconds,omitempty"`
	NoRedirect bool `json:"no_redirect,omitempty"`
}

type checkpointListResponse struct {
	Checkpoints []checkpointRecord `json:"checkpoints"`
}
