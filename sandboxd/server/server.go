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
	Release(ctx context.Context, id, token string) error
	AgentSocket(id, token string) (string, error)
	Info() ([]pool.PoolInfo, int)
}

// Dialer opens the hybrid-vsock connection to a VM's silkd.
type Dialer interface {
	DialSilkd(ctx context.Context, vsockSocket string) (net.Conn, error)
}

// InfoResponse is the wire reply of GET /v1/info.
type InfoResponse struct {
	Pools   []pool.PoolInfo `json:"pools"`
	Claimed int             `json:"claimed"`
}

// Server serves the control plane for one node.
type Server struct {
	mgr      Manager
	dialer   Dialer
	apiToken string

	relayMu     sync.Mutex
	relays      map[net.Conn]net.Conn // client conn → guest conn, for forced shutdown
	relayClosed bool
	relayWG     sync.WaitGroup
}

// New returns a Server; an empty apiToken leaves the node-level endpoints
// open (per-sandbox tokens still guard sandbox-scoped calls).
func New(apiToken string, mgr Manager, dialer Dialer) *Server {
	return &Server{
		mgr:      mgr,
		dialer:   dialer,
		apiToken: apiToken,
		relays:   map[net.Conn]net.Conn{},
	}
}

// Handler builds the route table.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/claim", s.requireAPIToken(s.handleClaim))
	mux.HandleFunc("POST /v1/sandboxes/{id}/release", s.handleRelease)
	mux.HandleFunc("GET /v1/sandboxes/{id}/agent", s.handleAgent)
	mux.HandleFunc("GET /v1/info", s.requireAPIToken(s.handleInfo))
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	return mux
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	var req types.ClaimRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	key := req.Key()
	if err := key.Validate(); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	sb, err := s.mgr.Claim(r.Context(), key, req.TTL())
	if errors.Is(err, pool.ErrNoEgress) {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		log.WithFunc("server.handleClaim").Errorf(r.Context(), err, "claim %s", key.Hash())
		writeErr(w, http.StatusInternalServerError, "provisioning failed")
		return
	}
	writeJSON(w, http.StatusOK, types.ClaimResponse{ID: sb.ID, Token: sb.Token, Deadline: sb.Deadline})
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

func (s *Server) handleInfo(w http.ResponseWriter, _ *http.Request) {
	pools, claimed := s.mgr.Info()
	writeJSON(w, http.StatusOK, InfoResponse{Pools: pools, Claimed: claimed})
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
