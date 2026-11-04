package server

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/store/peer"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const moveCallTimeout = 10 * time.Minute

var _ pool.MovePeer = (*MoveClient)(nil)

// MoveClient reaches a move's target on its control port with the root token.
type MoveClient struct {
	Token string
}

func (c *MoveClient) Admit(ctx context.Context, addr, id string, a pool.MoveAdmission) (string, error) {
	var out moveAdmitResponse
	err := c.call(ctx, http.MethodPost, addr, movePath(id, ""), moveAdmitRequest{Tenant: a.Tenant, Key: a.Key, From: a.From}, &out)
	return out.Node, err
}

func (c *MoveClient) SendRound(ctx context.Context, addr, id string, round int, dir string, files []string) (int64, error) {
	wire, err := peer.SendRound(ctx, addr, c.Token, movePath(id, "/rounds/"+strconv.Itoa(round)), dir, files)
	if err != nil {
		return wire, fmt.Errorf("%w: %w", pool.ErrMoveTarget, err)
	}
	return wire, nil
}

func (c *MoveClient) Commit(ctx context.Context, addr, id string, claim []byte, epoch int64) error {
	return c.call(ctx, http.MethodPost, addr, movePath(id, "/commit"), moveCommitRequest{Claim: claim, Epoch: epoch}, nil)
}

func (c *MoveClient) Drop(ctx context.Context, addr, id string) error {
	return c.call(ctx, http.MethodDelete, addr, movePath(id, ""), nil, nil)
}

// Maps the target's status codes onto pool errors.
func (c *MoveClient) call(ctx context.Context, method, addr, path string, in, out any) error {
	ctx, cancel := context.WithTimeout(ctx, moveCallTimeout)
	defer cancel()
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, peer.URL(addr, path), body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", pool.ErrMoveTarget, addr, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		if out == nil {
			return nil
		}
		return json.Unmarshal(raw, out)
	case http.StatusTooManyRequests:
		return fmt.Errorf("%w: target %s: %s", pool.ErrQuota, addr, targetReason(raw))
	case http.StatusConflict:
		return fmt.Errorf("%w: target %s: %s", pool.ErrNotMovable, addr, targetReason(raw))
	}
	return fmt.Errorf("%w: target %s answered %d: %s", pool.ErrMoveTarget, addr, resp.StatusCode, targetReason(raw))
}

// MoveRequest is the wire body of POST /v1/sandboxes/{id}/move.
type MoveRequest struct {
	To            string `json:"to"`
	MaxDowntimeMS int    `json:"max_downtime_ms,omitzero"`
}

// MoveResponse is the wire reply of POST /v1/sandboxes/{id}/move.
type MoveResponse struct {
	OwnerAddr  string `json:"owner_addr"`
	Rounds     int    `json:"rounds"`
	DowntimeMS int64  `json:"downtime_ms"`
}

type moveAdmitRequest struct {
	Tenant string        `json:"tenant,omitempty"`
	Key    types.PoolKey `json:"key"`
	From   string        `json:"from"`
}

type moveAdmitResponse struct {
	Node string `json:"node"`
}

type moveCommitRequest struct {
	Claim jsontext.Value `json:"claim"`
	Epoch int64          `json:"epoch"`
}

func (s *Server) handleMove(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBodyStrict[MoveRequest](w, r)
	if !ok {
		return
	}
	budget := time.Duration(req.MaxDowntimeMS) * time.Millisecond
	if req.MaxDowntimeMS != 0 && budget <= pool.MoveStartAllowance {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf("max_downtime_ms must be above %d: the target's receive and start alone take that long", pool.MoveStartAllowance.Milliseconds()))
		return
	}
	addr, ok := s.movePeerAddr(req.To)
	if !ok {
		writeErr(w, http.StatusBadRequest, "to must name a current mesh peer as GET /v1/peers lists it")
		return
	}
	id := r.PathValue("id")
	res, err := s.mgr.Move(r.Context(), id, addr, budget)
	writeResult(w, r, "move", id, "move failed", err, func() {
		writeJSON(w, http.StatusOK, MoveResponse{OwnerAddr: s.placer.ClientAddr(addr), Rounds: res.Rounds, DowntimeMS: res.Downtime.Milliseconds()})
	})
}

func (s *Server) handleAdmitMove(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBodyStrict[moveAdmitRequest](w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	node, err := s.mgr.AdmitMove(id, pool.MoveAdmission{Tenant: req.Tenant, Key: req.Key.Defaulted(), From: req.From})
	writeResult(w, r, "admit move", id, "admit move failed", err, func() {
		writeJSON(w, http.StatusOK, moveAdmitResponse{Node: node})
	})
}

func (s *Server) handleMoveRound(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := s.mgr.ReceiveMoveRound(id, r.Body)
	writeResult(w, r, "move round", id, "move round failed", err, noContent(w))
}

func (s *Server) handleCommitMove(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBodyStrict[moveCommitRequest](w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	err := s.mgr.CommitMove(r.Context(), id, req.Claim, req.Epoch)
	writeResult(w, r, "commit move", id, "commit move failed", err, noContent(w))
}

func (s *Server) handleDropMove(w http.ResponseWriter, r *http.Request) {
	s.mgr.DropMove(r.PathValue("id"))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) movePeerAddr(to string) (string, bool) {
	if s.placer == nil || to == "" {
		return "", false
	}
	for _, addr := range s.placer.PeerAddrs() {
		if addr == to || s.placer.ClientAddr(addr) == to {
			return addr, true
		}
	}
	return "", false
}

func movePath(id, suffix string) string {
	return "/v1/moves/" + url.PathEscape(id) + suffix
}

func targetReason(raw []byte) string {
	var body struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(raw, &body)
	return cmp.Or(body.Error, "no reason given")
}
