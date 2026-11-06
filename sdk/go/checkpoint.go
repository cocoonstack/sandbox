package sandbox

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"time"
)

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
	TTLSeconds int               `json:"ttl_seconds,omitzero"`
	NoRedirect bool              `json:"no_redirect,omitzero"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	OnExpire   string            `json:"on_expire,omitempty"`
	Env        map[string]EnvVar `json:"env,omitempty"`
}

type checkpointListResponse struct {
	Checkpoints []checkpointRecord `json:"checkpoints"`
}

// Checkpoint is a captured sandbox state bound to the node that holds it.
type Checkpoint struct {
	ID        string
	Name      string
	SandboxID string
	CreatedAt time.Time

	c    *Client
	addr string
}

// New claims a sandbox branched from the checkpoint, following an owner redirect.
func (ck *Checkpoint) New(ctx context.Context, opts ...Option) (*Sandbox, error) {
	var claim claimRequest
	for _, opt := range opts {
		opt(&claim)
	}
	if len(claim.Volumes) > 0 || claim.ClaimRef != "" {
		return nil, errors.New("checkpoint claims do not accept WithVolumes or WithClaimRef")
	}
	if err := claim.rejectPinnedAxes(); err != nil {
		return nil, err
	}
	addr, cr, err := claimFollow(ck.addr, "claim checkpoint", func(noRedirect, _ bool) ([]byte, error) {
		return encodeBody("checkpoint claim", checkpointClaimRequest{TTLSeconds: claim.TTLSeconds, NoRedirect: noRedirect, Metadata: claim.Metadata, OnExpire: claim.OnExpire, Env: claim.Env})
	}, func(a string, body []byte) (claimResponse, error) {
		return ck.claimAt(ctx, a, body)
	})
	if err != nil {
		return nil, err
	}
	return ck.c.handleFrom(addr, cr), nil
}

// Delete removes the checkpoint from its node; peer replicas are dropped best-effort.
func (ck *Checkpoint) Delete(ctx context.Context) error {
	return ck.c.doNoContent(ctx, http.MethodDelete, ck.addr, "/v1/checkpoints/"+ck.ID, nil, ck.c.apiToken, "delete checkpoint")
}

func (ck *Checkpoint) claimAt(ctx context.Context, addr string, body []byte) (claimResponse, error) {
	return ck.c.doJSON[claimResponse](ctx, http.MethodPost, addr, "/v1/checkpoints/"+ck.ID+"/claim", bytes.NewReader(body), ck.c.apiToken, "claim checkpoint")
}

// Checkpoint captures the sandbox's full state (memory, disk, running processes) without stopping it and returns a handle that branches new sandboxes from that moment.
func (s *Sandbox) Checkpoint(ctx context.Context, name string) (*Checkpoint, error) {
	body, err := encodeBody("checkpoint", checkpointRequest{Token: s.token, Name: name})
	if err != nil {
		return nil, err
	}
	cr, err := s.c.doJSON[checkpointResponse](ctx, http.MethodPost, s.owner, "/v1/sandboxes/"+s.ID+"/checkpoint", bytes.NewReader(body), s.c.apiToken, "checkpoint")
	if err != nil {
		return nil, err
	}
	return checkpointHandle(s.c, s.owner, cr.Checkpoint), nil
}

// Checkpoints lists the connected node's checkpoints, newest first, as handles bound to it.
func (c *Client) Checkpoints(ctx context.Context) ([]*Checkpoint, error) {
	lr, err := c.doJSON[checkpointListResponse](ctx, http.MethodGet, c.addr, "/v1/checkpoints", nil, c.apiToken, "list checkpoints")
	if err != nil {
		return nil, err
	}
	ckpts := make([]*Checkpoint, len(lr.Checkpoints))
	for i, rec := range lr.Checkpoints {
		ckpts[i] = checkpointHandle(c, c.addr, rec)
	}
	return ckpts, nil
}

// Checkpoint returns an entry-node handle for a known id; an unknown id surfaces as 404 at claim.
func (c *Client) Checkpoint(id string) *Checkpoint {
	return checkpointHandle(c, c.addr, checkpointRecord{ID: id})
}

func checkpointHandle(c *Client, addr string, rec checkpointRecord) *Checkpoint {
	return &Checkpoint{
		ID: rec.ID, Name: rec.Name, SandboxID: rec.SandboxID, CreatedAt: rec.CreatedAt,
		c: c, addr: addr,
	}
}
