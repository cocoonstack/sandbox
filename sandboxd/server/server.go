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

// poolErrHTTP maps pool sentinels to their HTTP replies; an empty msg
// surfaces err.Error() (4xx detail the caller can act on), a fixed msg
// avoids echoing internals on lookups.
var poolErrHTTP = []struct {
	err  error
	code int
	msg  string
}{
	{pool.ErrBadKey, http.StatusBadRequest, ""},
	{pool.ErrBadCount, http.StatusBadRequest, ""},
	{pool.ErrNoEgress, http.StatusConflict, ""},
	{pool.ErrPooledTemplate, http.StatusConflict, ""},
	{pool.ErrUnknownSandbox, http.StatusNotFound, "unknown sandbox"},
	{pool.ErrUnknownTemplate, http.StatusNotFound, "unknown template"},
}

// Manager is the slice of the pool manager the server consumes.
type Manager interface {
	ClaimWarm(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error)
	ClaimProvision(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error)
	Release(ctx context.Context, id, token string) error
	Hibernate(ctx context.Context, id, token string) error
	Fork(ctx context.Context, id, token string, count int, ttl time.Duration) ([]*types.Sandbox, error)
	Promote(ctx context.Context, id, token, template string) (types.PoolKey, error)
	DeleteTemplate(key types.PoolKey) error
	HasGolden(key types.PoolKey) bool
	AgentSocket(id, token string) (string, error)
	WakeAgentSocket(ctx context.Context, id, token string) (string, error)
	Info() ([]pool.PoolInfo, int, int)
}

// Dialer opens the hybrid-vsock connection to a VM's silkd.
type Dialer interface {
	DialSilkd(ctx context.Context, vsockSocket string) (net.Conn, error)
}

// Placer names peers for redirect placement and lists the mesh for Lookup;
// nil on a single-node deployment (no mesh).
type Placer interface {
	Candidates(keyHash string) []string
	TemplateOwners(keyHash string) []string
	PeerAddrs() []string
}

// InfoResponse is the wire reply of GET /v1/info. Peers lists the other nodes'
// data-plane addresses so a client can scatter a Lookup across the cluster.
type InfoResponse struct {
	Pools      []pool.PoolInfo `json:"pools"`
	Claimed    int             `json:"claimed"`
	Hibernated int             `json:"hibernated"`
	Peers      []string        `json:"peers,omitempty"`
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
	mux.HandleFunc("POST /v1/sandboxes/{id}/release", s.handleSandboxVerb("release", s.mgr.Release))
	mux.HandleFunc("POST /v1/sandboxes/{id}/hibernate", s.handleSandboxVerb("hibernate", s.mgr.Hibernate))
	// Fork and promote create node resources, so they take the api token
	// like a claim; the source sandbox's token rides in the body as the
	// ownership proof.
	mux.HandleFunc("POST /v1/sandboxes/{id}/fork", s.requireAPIToken(s.handleFork))
	mux.HandleFunc("POST /v1/sandboxes/{id}/promote", s.requireAPIToken(s.handlePromote))
	mux.HandleFunc("DELETE /v1/templates", s.requireAPIToken(s.handleDeleteTemplate))
	mux.HandleFunc("GET /v1/sandboxes/{id}/agent", s.handleAgent)
	mux.HandleFunc("GET /v1/sandboxes/{id}/owner", s.handleOwner)
	mux.HandleFunc("GET /v1/info", s.requireAPIToken(s.handleInfo))
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	return mux
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBody[types.ClaimRequest](w, r)
	if !ok {
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
			if writeRedirect(w, s.placer.Candidates(key.Hash())) {
				return
			}
			// A promoted template lives only on its owner node: when this
			// node has no golden for the key but gossip names one, the owner
			// provisions from it instead of us cold-booting a nonexistent
			// image ref.
			if !s.mgr.HasGolden(key) && writeRedirect(w, s.placer.TemplateOwners(key.Hash())) {
				return
			}
		}
		sb, err = s.mgr.ClaimProvision(r.Context(), key, req.TTL())
	}
	switch {
	case writePoolErr(w, err):
	case err != nil:
		log.WithFunc("server.handleClaim").Errorf(r.Context(), err, "claim %s", key.Hash())
		writeErr(w, http.StatusInternalServerError, "provisioning failed")
	default:
		writeJSON(w, http.StatusOK, s.claimResponse(sb))
	}
}

// handleSandboxVerb adapts a sandbox-scoped manager call (release,
// hibernate) to HTTP: per-sandbox bearer auth, 404 on unknown, 204 on
// success.
func (s *Server) handleSandboxVerb(verb string, do func(ctx context.Context, id, token string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := sandboxToken(w, r)
		if !ok {
			return
		}
		id := r.PathValue("id")
		err := do(r.Context(), id, token)
		switch {
		case writePoolErr(w, err):
		case err != nil:
			log.WithFunc("server.handleSandboxVerb").Errorf(r.Context(), err, "%s %s", verb, id)
			writeErr(w, http.StatusInternalServerError, verb+" failed")
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}
}

// handleFork clones a claimed sandbox into fresh child claims, one
// ClaimResponse per child; this node owns them all.
func (s *Server) handleFork(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBody[types.ForkRequest](w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	children, err := s.mgr.Fork(r.Context(), id, req.Token, req.Count, req.TTL())
	switch {
	case writePoolErr(w, err):
	case err != nil:
		log.WithFunc("server.handleFork").Errorf(r.Context(), err, "fork %s", id)
		writeErr(w, http.StatusInternalServerError, "fork failed")
	default:
		resp := types.ForkResponse{Children: make([]types.ClaimResponse, len(children))}
		for i, c := range children {
			resp.Children[i] = s.claimResponse(c)
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

// handlePromote publishes a claimed sandbox as a node-local template.
func (s *Server) handlePromote(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBody[types.PromoteRequest](w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	key, err := s.mgr.Promote(r.Context(), id, req.Token, req.Template)
	switch {
	case writePoolErr(w, err):
	case err != nil:
		log.WithFunc("server.handlePromote").Errorf(r.Context(), err, "promote %s", id)
		writeErr(w, http.StatusInternalServerError, "promote failed")
	default:
		writeJSON(w, http.StatusOK, types.PromoteResponse{Key: key})
	}
}

// handleDeleteTemplate removes a promoted template; the key axes ride as
// query parameters, defaulted like a claim's.
func (s *Server) handleDeleteTemplate(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	req := types.ClaimRequest{
		Template: q.Get("template"),
		Net:      types.NetShape(q.Get("net")),
		Size:     types.Size(q.Get("size")),
	}
	key := req.Key()
	err := s.mgr.DeleteTemplate(key)
	// Unknown here but owned by a peer per gossip: redirect the SDK to the
	// owner. no_redirect mirrors the claim protocol — a redirected retry
	// carries it, so the owner answers for itself and never bounces again.
	if errors.Is(err, pool.ErrUnknownTemplate) && s.placer != nil && q.Get("no_redirect") == "" &&
		writeRedirect(w, s.placer.TemplateOwners(key.Hash())) {
		return
	}
	switch {
	case writePoolErr(w, err):
	case err != nil:
		log.WithFunc("server.handleDeleteTemplate").Errorf(r.Context(), err, "delete template %s", req.Template)
		writeErr(w, http.StatusInternalServerError, "delete template failed")
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
	pools, claimed, hibernated := s.mgr.Info()
	resp := InfoResponse{Pools: pools, Claimed: claimed, Hibernated: hibernated}
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

// sandboxToken extracts the per-sandbox bearer token, answering 401 itself.
func sandboxToken(w http.ResponseWriter, r *http.Request) (string, bool) {
	token, ok := bearerToken(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "missing bearer token")
	}
	return token, ok
}

// decodeBody parses a JSON request body, answering 400 itself on failure.
func decodeBody[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var v T
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return v, false
	}
	return v, true
}

func (s *Server) claimResponse(sb *types.Sandbox) types.ClaimResponse {
	return types.ClaimResponse{ID: sb.ID, Token: sb.Token, Deadline: sb.Deadline, OwnerAddr: s.advertise}
}

// writePoolErr answers a pool-sentinel error per poolErrHTTP, reporting
// whether it handled err; nil and non-sentinel errors stay the caller's.
func writePoolErr(w http.ResponseWriter, err error) bool {
	for _, m := range poolErrHTTP {
		if errors.Is(err, m.err) {
			msg := m.msg
			if msg == "" {
				msg = err.Error()
			}
			writeErr(w, m.code, msg)
			return true
		}
	}
	return false
}

// writeRedirect answers with the redirect shape of the claim protocol when
// addrs is non-empty, reporting whether it did.
func writeRedirect(w http.ResponseWriter, addrs []string) bool {
	if len(addrs) == 0 {
		return false
	}
	writeJSON(w, http.StatusOK, types.ClaimResponse{Redirect: addrs})
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
