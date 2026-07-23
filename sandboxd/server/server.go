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
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	upgradeProto = "silkd"
	maxBodyBytes = 1 << 20
	previewTTL   = time.Hour
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
	{pool.ErrBadName, http.StatusBadRequest, ""},
	{pool.ErrBadCount, http.StatusBadRequest, ""},
	{pool.ErrNoEgress, http.StatusConflict, ""},
	{pool.ErrNoEgressHibernate, http.StatusConflict, ""},
	{pool.ErrNoEgressFork, http.StatusConflict, ""},
	{pool.ErrQuota, http.StatusTooManyRequests, ""},
	{pool.ErrPooledTemplate, http.StatusConflict, ""},
	{pool.ErrTemplateOwned, http.StatusConflict, ""},
	{pool.ErrUnknownSandbox, http.StatusNotFound, "unknown sandbox"},
	{pool.ErrUnknownTemplate, http.StatusNotFound, "unknown template"},
	{pool.ErrUnknownCheckpoint, http.StatusNotFound, "unknown checkpoint"},
}

// Manager is the slice of the pool manager the server consumes. tenant
// parameters attribute created resources and scope listings/deletes; empty
// means the operator (root) — unquotaed, unfiltered.
type Manager interface {
	ClaimWarm(ctx context.Context, key types.PoolKey, ttl time.Duration, tenant, claimRef string) (*types.Sandbox, error)
	ClaimProvision(ctx context.Context, key types.PoolKey, ttl time.Duration, tenant, claimRef string) (*types.Sandbox, error)
	Release(ctx context.Context, id, token string) error
	ReleaseOperator(ctx context.Context, id string) error
	Hibernate(ctx context.Context, id, token string) error
	Fork(ctx context.Context, id, token string, count int, ttl time.Duration) ([]*types.Sandbox, error)
	Promote(ctx context.Context, id, token, template, tenant string) (types.PoolKey, error)
	DeleteTemplate(ctx context.Context, key types.PoolKey, tenant string) error
	Checkpoint(ctx context.Context, id, token, name, tenant string) (types.Checkpoint, error)
	Counters() pool.Counters
	TenantClaims() map[string]int
	Sandboxes() []pool.SandboxSummary
	Audit(ctx context.Context, id string, line []byte)
	AuditEnabled() bool
	ClaimCheckpoint(ctx context.Context, ckptID string, ttl time.Duration, tenant string) (*types.Sandbox, error)
	Checkpoints(ctx context.Context, tenant string) ([]types.Checkpoint, error)
	DeleteCheckpoint(ctx context.Context, ckptID, tenant string) error
	ClaimDeadline(id, token string) (time.Time, error)
	HasGolden(ctx context.Context, key types.PoolKey) bool
	AgentSocket(id, token string) (string, error)
	WakeAgentSocket(ctx context.Context, id, token string) (string, error)
	SetPools(ctx context.Context, pools []config.PoolSpec) error
	Drain(ctx context.Context)
	Uncordon(ctx context.Context)
	Info() ([]pool.PoolInfo, pool.Gauges)
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
	ConfigMismatches() int
}

// InfoResponse is the wire reply of GET /v1/info. Peers lists the other nodes'
// data-plane addresses so a client can scatter a Lookup across the cluster.
type InfoResponse struct {
	Pools      []pool.PoolInfo `json:"pools"`
	Claimed    int             `json:"claimed"`
	Hibernated int             `json:"hibernated"`
	Archived   int             `json:"archived"`
	Draining   bool            `json:"draining,omitempty"`
	Peers      []string        `json:"peers,omitempty"`
}

// PoolUpdateRequest is the wire body of PUT /v1/pools. It replaces the node's
// desired warm targets with the supplied list; omitted pools are drained.
// Lives here, not types/api.go: it embeds config.PoolSpec and types cannot
// import config.
type PoolUpdateRequest struct {
	Pools []config.PoolSpec `json:"pools"`
}

// Server serves the control plane for one node.
type Server struct {
	mgr       Manager
	dialer    Dialer
	placer    Placer
	apiToken  string
	tenants   []config.TenantSpec
	advertise string
	preview   *PreviewServer

	relayMu     sync.Mutex
	relays      map[net.Conn]net.Conn // client conn → guest conn, for forced shutdown
	relayClosed bool
	relayWG     sync.WaitGroup
}

// New returns a Server; an empty apiToken with no tenants leaves the
// node-level endpoints open (per-sandbox tokens still guard sandbox-scoped
// calls). tenants adds per-tenant tokens next to the root apiToken.
// advertise is this node's data-plane address, returned as a claim's owner
// address. A nil placer disables mesh redirects (single node).
func New(apiToken string, tenants []config.TenantSpec, advertise string, mgr Manager, dialer Dialer, placer Placer, preview *PreviewServer) *Server {
	return &Server{
		mgr:       mgr,
		dialer:    dialer,
		placer:    placer,
		apiToken:  apiToken,
		tenants:   tenants,
		advertise: advertise,
		preview:   preview,
		relays:    map[net.Conn]net.Conn{},
	}
}

// Handler builds the route table. Resource-creating verbs and tenant-scoped
// listings/deletes take the api token or a tenant token; operator surfaces
// (index, pools, info, metrics) stay root-only.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/claim", s.requireToken(s.handleClaim))
	mux.HandleFunc("POST /v1/sandboxes/{id}/release", s.handleRelease)
	mux.HandleFunc("POST /v1/sandboxes/{id}/hibernate", s.handleSandboxVerb("hibernate", s.mgr.Hibernate))
	// Fork and promote create node resources, so they take the same token
	// class as a claim; the source sandbox's token rides in the body as the
	// ownership proof.
	mux.HandleFunc("POST /v1/sandboxes/{id}/fork", s.requireToken(s.handleFork))
	mux.HandleFunc("POST /v1/sandboxes/{id}/promote", s.requireToken(s.handlePromote))
	mux.HandleFunc("POST /v1/sandboxes/{id}/preview", s.requireToken(s.handlePreview))
	mux.HandleFunc("POST /v1/sandboxes/{id}/checkpoint", s.requireToken(s.handleCheckpoint))
	mux.HandleFunc("POST /v1/checkpoints/{id}/claim", s.requireToken(s.handleClaimCheckpoint))
	mux.HandleFunc("GET /v1/checkpoints", s.requireToken(s.handleListCheckpoints))
	mux.HandleFunc("DELETE /v1/checkpoints/{id}", s.requireToken(s.handleDeleteCheckpoint))
	mux.HandleFunc("DELETE /v1/templates", s.requireToken(s.handleDeleteTemplate))
	mux.HandleFunc("PUT /v1/pools", s.requireRoot(s.handlePutPools))
	mux.HandleFunc("POST /v1/drain", s.requireRoot(s.handleDrain))
	mux.HandleFunc("DELETE /v1/drain", s.requireRoot(s.handleUncordon))
	mux.HandleFunc("GET /v1/sandboxes/{id}/agent", s.handleAgent)
	mux.HandleFunc("GET /v1/sandboxes/{id}/owner", s.handleOwner)
	mux.HandleFunc("GET /v1/info", s.requireRoot(s.handleInfo))
	mux.HandleFunc("GET /v1/peers", s.requireToken(s.handlePeers))
	mux.HandleFunc("GET /v1/sandboxes", s.requireRoot(s.handleSandboxes))
	mux.HandleFunc("GET /metrics", s.requireRoot(s.handleMetrics))
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	return mux
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBody[types.ClaimRequest](w, r)
	if !ok {
		return
	}
	key := req.Key()
	hash := key.Hash()

	// Warm hit here is ownership transfer only. On a warm miss with a mesh, a
	// peer that reports a warm sandbox gets the claim via redirect (data plane
	// must be direct, so redirect beats proxy); only if no peer has one does
	// this node provision (golden clone or cold boot).
	tenant := tenantFrom(r.Context())
	sb, err := s.mgr.ClaimWarm(r.Context(), key, req.TTL(), tenant, req.ClaimRef)
	if errors.Is(err, pool.ErrNoWarm) {
		if s.redirectClaim(r.Context(), w, req, key, hash) {
			return
		}
		sb, err = s.mgr.ClaimProvision(r.Context(), key, req.TTL(), tenant, req.ClaimRef)
	}
	// A full node bounces the claim to a warm peer before answering 429 —
	// quota is per node, and a peer with capacity is a better answer.
	if errors.Is(err, pool.ErrQuota) && s.placer != nil && !req.NoRedirect &&
		writeRedirect(w, s.placer.Candidates(hash)) {
		return
	}
	writeResult(w, r, "claim", hash, "provisioning failed", err, func() {
		writeJSON(w, http.StatusOK, s.claimResponse(sb))
	})
}

// redirectClaim redirects a warm-miss to a better peer — a warm holder, or the
// template owner when we lack a golden (so we don't cold-boot a nonexistent
// image ref). A no_redirect request must resolve locally, never bounce again,
// to avoid a two-node ping-pong.
func (s *Server) redirectClaim(ctx context.Context, w http.ResponseWriter, req types.ClaimRequest, key types.PoolKey, hash string) bool {
	if s.placer == nil || req.NoRedirect {
		return false
	}
	if writeRedirect(w, s.placer.Candidates(hash)) {
		return true
	}
	// TemplateOwners is in-memory; HasGolden can be a store round-trip.
	owners := s.placer.TemplateOwners(hash)
	return len(owners) > 0 && !s.mgr.HasGolden(ctx, key) && writeRedirect(w, owners)
}

// handleRelease releases a claimed sandbox. Two credentials authorize it: the
// node's root api_token (the operator) may release any sandbox by id, so
// aggregated/control-plane teardown works without holding the per-sandbox token;
// a per-sandbox token releases only its own claim, unchanged. A tenant token is
// neither — it is not the root api_token, so it takes the per-sandbox path and
// 404s (it matches no sandbox token). Tenants never get operator release.
func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	token, ok := sandboxToken(w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	var err error
	if s.isRootToken(token) {
		err = s.mgr.ReleaseOperator(r.Context(), id)
	} else {
		err = s.mgr.Release(r.Context(), id, token)
	}
	switch {
	case writePoolErr(w, err):
	case err != nil:
		log.WithFunc("server.handleRelease").Errorf(r.Context(), err, "release %s", id)
		writeErr(w, http.StatusInternalServerError, "release failed")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// handleSandboxVerb adapts a sandbox-scoped manager call (hibernate) to HTTP:
// per-sandbox bearer auth, 404 on unknown, 204 on success.
func (s *Server) handleSandboxVerb(verb string, do func(ctx context.Context, id, token string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := sandboxToken(w, r)
		if !ok {
			return
		}
		id := r.PathValue("id")
		err := do(r.Context(), id, token)
		writeResult(w, r, verb, id, verb+" failed", err, func() {
			w.WriteHeader(http.StatusNoContent)
		})
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
	writeResult(w, r, "fork", id, "fork failed", err, func() {
		resp := types.ForkResponse{Children: make([]types.ClaimResponse, len(children))}
		for i, c := range children {
			resp.Children[i] = s.claimResponse(c)
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

// handlePromote publishes a claimed sandbox as a node-local template.
func (s *Server) handlePromote(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBody[types.PromoteRequest](w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	key, err := s.mgr.Promote(r.Context(), id, req.Token, req.Template, tenantFrom(r.Context()))
	writeResult(w, r, "promote", id, "promote failed", err, func() {
		writeJSON(w, http.StatusOK, types.PromoteResponse{Key: key})
	})
}

// handleCheckpoint captures a claimed sandbox's state as a new checkpoint.
func (s *Server) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBody[types.CheckpointRequest](w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	ckpt, err := s.mgr.Checkpoint(r.Context(), id, req.Token, req.Name, tenantFrom(r.Context()))
	writeResult(w, r, "checkpoint", id, "checkpoint failed", err, func() {
		writeJSON(w, http.StatusOK, types.CheckpointResponse{Checkpoint: ckpt})
	})
}

// handleClaimCheckpoint claims a fresh sandbox branched from a checkpoint.
func (s *Server) handleClaimCheckpoint(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBody[types.CheckpointClaimRequest](w, r)
	if !ok {
		return
	}
	ckptID := r.PathValue("id")
	sb, err := s.mgr.ClaimCheckpoint(r.Context(), ckptID, req.TTL(), tenantFrom(r.Context()))
	writeResult(w, r, "claim checkpoint", ckptID, "provisioning failed", err, func() {
		writeJSON(w, http.StatusOK, s.claimResponse(sb))
	})
}

// handleListCheckpoints lists this node's checkpoints, newest first — a
// tenant caller sees only its own records.
func (s *Server) handleListCheckpoints(w http.ResponseWriter, r *http.Request) {
	ckpts, err := s.mgr.Checkpoints(r.Context(), tenantFrom(r.Context()))
	if err != nil {
		log.WithFunc("server.handleListCheckpoints").Error(r.Context(), err, "list checkpoints")
		writeErr(w, http.StatusInternalServerError, "list checkpoints failed")
		return
	}
	writeJSON(w, http.StatusOK, types.CheckpointListResponse{Checkpoints: ckpts})
}

// handleDeleteCheckpoint removes a checkpoint; a tenant caller may delete
// only its own records (anything else is 404).
func (s *Server) handleDeleteCheckpoint(w http.ResponseWriter, r *http.Request) {
	err := s.mgr.DeleteCheckpoint(r.Context(), r.PathValue("id"), tenantFrom(r.Context()))
	writeResult(w, r, "delete checkpoint", r.PathValue("id"), "delete checkpoint failed", err, func() {
		w.WriteHeader(http.StatusNoContent)
	})
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
	err := s.mgr.DeleteTemplate(r.Context(), key, tenantFrom(r.Context()))
	// Unknown here but owned by a peer per gossip: redirect the SDK to the
	// owner. no_redirect mirrors the claim protocol — a redirected retry
	// carries it, so the owner answers for itself and never bounces again.
	if errors.Is(err, pool.ErrUnknownTemplate) && s.placer != nil && q.Get("no_redirect") == "" &&
		writeRedirect(w, s.placer.TemplateOwners(key.Hash())) {
		return
	}
	writeResult(w, r, "delete template", req.Template, "delete template failed", err, func() {
		w.WriteHeader(http.StatusNoContent)
	})
}

// handleOwner answers whether this node owns the sandbox (used by the SDK's
// Lookup scatter to relocate a handle whose owner address was lost). The
// per-sandbox token both authorizes the query and confirms ownership.
func (s *Server) handleOwner(w http.ResponseWriter, r *http.Request) {
	token, ok := sandboxToken(w, r)
	if !ok {
		return
	}
	if _, err := s.mgr.AgentSocket(r.PathValue("id"), token); err != nil {
		writeErr(w, http.StatusNotFound, "not owned here")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"owner_addr": s.advertise})
}

func (s *Server) handlePutPools(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBodyStrict[PoolUpdateRequest](w, r)
	if !ok {
		return
	}
	err := s.mgr.SetPools(r.Context(), req.Pools)
	switch {
	case writePoolErr(w, err):
	case err != nil:
		log.WithFunc("server.handlePutPools").Error(r.Context(), err, "set pools")
		writeErr(w, http.StatusInternalServerError, "set pools failed")
	default:
		s.handleInfo(w, r)
	}
}

func (s *Server) handleDrain(w http.ResponseWriter, r *http.Request) {
	s.mgr.Drain(r.Context())
	s.handleInfo(w, r)
}

func (s *Server) handleUncordon(w http.ResponseWriter, r *http.Request) {
	s.mgr.Uncordon(r.Context())
	s.handleInfo(w, r)
}

// handlePeers lists the cluster's node addresses for the SDK's redirect
// follow and Lookup scatter — cluster topology, not operator state, so any
// valid token (root or tenant) may read it.
func (s *Server) handlePeers(w http.ResponseWriter, _ *http.Request) {
	var peers []string
	if s.placer != nil {
		peers = s.placer.PeerAddrs()
	}
	writeJSON(w, http.StatusOK, map[string][]string{"peers": peers})
}

func (s *Server) handleInfo(w http.ResponseWriter, _ *http.Request) {
	pools, g := s.mgr.Info()
	resp := InfoResponse{Pools: pools, Claimed: g.Claimed, Hibernated: g.Hibernated, Archived: g.Archived, Draining: g.Draining}
	if s.placer != nil {
		resp.Peers = s.placer.PeerAddrs()
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = io.WriteString(w, "ok")
}

// requireToken guards node-level endpoints, resolving the caller's scope —
// root (the api token) or a tenant name (its configured token) — onto the
// request context; sandbox-scoped endpoints use per-sandbox tokens instead.
func (s *Server) requireToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenant, ok := s.resolveScope(r)
		if !ok {
			writeErr(w, http.StatusUnauthorized, "invalid api token")
			return
		}
		next(w, r.WithContext(withTenant(r.Context(), tenant)))
	}
}

// requireRoot guards operator-only surfaces: a tenant token authenticates
// but is not authorized, so it gets 403, never 401.
func (s *Server) requireRoot(next http.HandlerFunc) http.HandlerFunc {
	return s.requireToken(func(w http.ResponseWriter, r *http.Request) {
		if tenantFrom(r.Context()) != "" {
			writeErr(w, http.StatusForbidden, "operator token required")
			return
		}
		next(w, r)
	})
}

// isRootToken reports whether token is the configured root api_token (the
// operator credential). It mirrors resolveScope's root branch: an unset api
// token matches nothing, so an open node grants no operator elevation on the
// per-sandbox release path. Tenant tokens never match — they live in s.tenants.
func (s *Server) isRootToken(token string) bool {
	return s.apiToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.apiToken)) == 1
}

// resolveScope matches the bearer token to root ("") or a tenant name. With
// no api token and no tenants the node-level endpoints stay open.
func (s *Server) resolveScope(r *http.Request) (string, bool) {
	if s.apiToken == "" && len(s.tenants) == 0 {
		return "", true
	}
	token, ok := bearerToken(r)
	if !ok {
		return "", false
	}
	if s.apiToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.apiToken)) == 1 {
		return "", true
	}
	for _, tn := range s.tenants {
		if subtle.ConstantTimeCompare([]byte(token), []byte(tn.Token)) == 1 {
			return tn.Name, true
		}
	}
	return "", false
}

func (s *Server) claimResponse(sb *types.Sandbox) types.ClaimResponse {
	return types.ClaimResponse{
		ID: sb.ID, Token: sb.Token, Deadline: sb.Deadline,
		OwnerAddr: s.advertise, FromCheckpoint: sb.FromCheckpoint,
	}
}
