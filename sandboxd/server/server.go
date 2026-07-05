// Package server exposes the v0 control plane: claim/release/info over
// HTTP/JSON, plus the data plane — an HTTP Upgrade relayed byte-for-byte
// between the client and the guest's silkd vsock port.
//
// The owning http.Server must keep ReadTimeout and WriteTimeout at zero: a
// cold-key claim legitimately blocks for the cold probe timeout and relays
// stream indefinitely. Use ReadHeaderTimeout for slowloris protection.
package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	upgradeProto = "silkd"
	maxBodyBytes = 1 << 20
)

// Manager is the slice of the pool manager the server consumes.
type Manager interface {
	Claim(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error)
	ClaimWarm(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error)
	ClaimProvision(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error)
	Release(ctx context.Context, id, token string) error
	AgentSocket(id, token string) (string, error)
	Info() ([]pool.PoolInfo, int)
}

// Dialer opens the hybrid-vsock connection to a VM's silkd.
type Dialer interface {
	DialSilkd(ctx context.Context, vsockSocket string) (net.Conn, error)
}

// Placer names peers for redirect placement and lists the mesh for Lookup;
// nil on a single-node deployment (no mesh).
type Placer interface {
	Candidates(keyHash string) []string
	PeerAddrs() []string
}

// InfoResponse is the wire reply of GET /v1/info. Peers lists the other nodes'
// data-plane addresses so a client can scatter a Lookup across the cluster.
type InfoResponse struct {
	Pools   []pool.PoolInfo `json:"pools"`
	Claimed int             `json:"claimed"`
	Peers   []string        `json:"peers,omitempty"`
}

// Server serves the control plane for one node.
type Server struct {
	mgr       Manager
	dialer    Dialer
	placer    Placer
	apiToken  string
	advertise string

	relayMu     sync.Mutex
	relays      map[net.Conn]net.Conn // client conn → guest conn, for forced shutdown
	relayClosed bool
	relayWG     sync.WaitGroup
}

// New returns a Server; an empty apiToken leaves the node-level endpoints
// open (per-sandbox tokens still guard sandbox-scoped calls). advertise is
// this node's data-plane address, returned as a claim's owner address. A nil
// placer disables mesh redirects (single node).
func New(apiToken, advertise string, mgr Manager, dialer Dialer, placer Placer) *Server {
	return &Server{
		mgr:       mgr,
		dialer:    dialer,
		placer:    placer,
		apiToken:  apiToken,
		advertise: advertise,
		relays:    map[net.Conn]net.Conn{},
	}
}

// Handler builds the route table.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/claim", s.requireAPIToken(s.handleClaim))
	mux.HandleFunc("POST /v1/sandboxes/{id}/release", s.handleRelease)
	mux.HandleFunc("GET /v1/sandboxes/{id}/agent", s.handleAgent)
	mux.HandleFunc("GET /v1/sandboxes/{id}/owner", s.handleOwner)
	mux.HandleFunc("GET /v1/info", s.requireAPIToken(s.handleInfo))
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	return mux
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	var req types.ClaimRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	key := req.Key()

	// Warm hit here is ownership transfer only. On a warm miss with a mesh, a
	// peer that reports a warm sandbox gets the claim via redirect (data plane
	// must be direct, so redirect beats proxy); only if no peer has one does
	// this node provision (golden clone or cold boot).
	sb, err := s.mgr.ClaimWarm(r.Context(), key, req.TTL())
	if errors.Is(err, pool.ErrNoWarm) {
		// A claim already redirected to us must warm-or-provision here (confirm
		// at owner), never bounce again — that avoids a two-node stale-view
		// ping-pong when both just emptied their pools.
		if s.placer != nil && !req.NoRedirect {
			if addrs := s.placer.Candidates(key.Hash()); len(addrs) > 0 {
				writeJSON(w, http.StatusOK, types.ClaimResponse{Redirect: addrs})
				return
			}
		}
		sb, err = s.mgr.ClaimProvision(r.Context(), key, req.TTL())
	}
	switch {
	case errors.Is(err, pool.ErrBadKey):
		writeErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, pool.ErrNoEgress):
		writeErr(w, http.StatusConflict, err.Error())
	case err != nil:
		log.WithFunc("server.handleClaim").Errorf(r.Context(), err, "claim %s", key.Hash())
		writeErr(w, http.StatusInternalServerError, "provisioning failed")
	default:
		writeJSON(w, http.StatusOK, types.ClaimResponse{
			ID: sb.ID, Token: sb.Token, Deadline: sb.Deadline, OwnerAddr: s.advertise,
		})
	}
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	token, ok := bearerToken(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	id := r.PathValue("id")
	err := s.mgr.Release(r.Context(), id, token)
	switch {
	case errors.Is(err, pool.ErrUnknownSandbox):
		writeErr(w, http.StatusNotFound, "unknown sandbox")
	case err != nil:
		log.WithFunc("server.handleRelease").Errorf(r.Context(), err, "release %s", id)
		writeErr(w, http.StatusInternalServerError, "release failed")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// handleOwner answers whether this node owns the sandbox (used by the SDK's
// Lookup scatter to relocate a handle whose owner address was lost). The
// per-sandbox token both authorizes the query and confirms ownership.
func (s *Server) handleOwner(w http.ResponseWriter, r *http.Request) {
	token, ok := bearerToken(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	if _, err := s.mgr.AgentSocket(r.PathValue("id"), token); err != nil {
		writeErr(w, http.StatusNotFound, "not owned here")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"owner_addr": s.advertise})
}

func (s *Server) handleInfo(w http.ResponseWriter, _ *http.Request) {
	pools, claimed := s.mgr.Info()
	resp := InfoResponse{Pools: pools, Claimed: claimed}
	if s.placer != nil {
		resp.Peers = s.placer.PeerAddrs()
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = io.WriteString(w, "ok")
}

// requireAPIToken guards node-level endpoints with the configured token;
// sandbox-scoped endpoints use per-sandbox tokens instead.
func (s *Server) requireAPIToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.apiToken != "" {
			token, ok := bearerToken(r)
			if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(s.apiToken)) != 1 {
				writeErr(w, http.StatusUnauthorized, "invalid api token")
				return
			}
		}
		next(w, r)
	}
}

func bearerToken(r *http.Request) (string, bool) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return token, ok && token != ""
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
