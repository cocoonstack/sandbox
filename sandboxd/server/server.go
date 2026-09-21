// Package server exposes the v0 control plane: claim/release/info over HTTP/JSON, plus the data plane — an HTTP Upgrade relayed byte-for-byte between the client and the guest's silkd vsock port.
package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"sync"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/store/peer"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	upgradeProto = "silkd"
	// upgradeProtoTCP names the guest-port passthrough: raw bytes, no framing of ours.
	upgradeProtoTCP = "tcp"
	maxBodyBytes    = 1 << 20
	previewTTL      = time.Hour
)

var poolErrHTTP = []struct {
	err  error
	code int
	msg  string
}{
	{pool.ErrBadKey, http.StatusBadRequest, ""},
	{pool.ErrBadVolume, http.StatusBadRequest, ""},
	{pool.ErrBadName, http.StatusBadRequest, ""},
	{pool.ErrBadCount, http.StatusBadRequest, ""},
	{pool.ErrNoEgress, http.StatusConflict, ""},
	{pool.ErrNoEgressHibernate, http.StatusConflict, ""},
	{pool.ErrNoEgressFork, http.StatusConflict, ""},
	{pool.ErrVolumeCapture, http.StatusConflict, ""},
	{pool.ErrVolumeBusy, http.StatusConflict, ""},
	{pool.ErrVolumeNeedsRecovery, http.StatusConflict, ""},
	{pool.ErrArchived, http.StatusConflict, ""},
	{pool.ErrQuota, http.StatusTooManyRequests, ""},
	{pool.ErrHealBusy, http.StatusServiceUnavailable, ""},
	{pool.ErrPooledTemplate, http.StatusConflict, ""},
	{pool.ErrTemplateOwned, http.StatusConflict, ""},
	{pool.ErrUnknownSandbox, http.StatusNotFound, "unknown sandbox"},
	{pool.ErrUnknownTemplate, http.StatusNotFound, "unknown template"},
	{pool.ErrUnknownCheckpoint, http.StatusNotFound, "unknown checkpoint"},
}

// Manager is the pool manager slice the server consumes; an empty tenant means operator (root).
type Manager interface {
	ClaimWarm(ctx context.Context, key types.PoolKey, ttl time.Duration, tenant, claimRef string, volumes []types.Volume) (*types.Sandbox, error)
	ClaimProvision(ctx context.Context, key types.PoolKey, ttl time.Duration, tenant, claimRef string, volumes []types.Volume) (*types.Sandbox, error)
	ClaimProvisionPromoted(ctx context.Context, key types.PoolKey, ttl time.Duration, tenant, claimRef string, volumes []types.Volume) (*types.Sandbox, error)
	Release(ctx context.Context, id string, cred pool.Cred) error
	Hibernate(ctx context.Context, id string, cred pool.Cred) error
	Wake(ctx context.Context, id string, cred pool.Cred) error
	Renew(ctx context.Context, id string, cred pool.Cred, ttl time.Duration) (time.Time, error)
	Fork(ctx context.Context, id string, cred pool.Cred, count int, ttl time.Duration) ([]*types.Sandbox, error)
	Promote(ctx context.Context, id string, cred pool.Cred, template, tenant string) (types.PoolKey, string, error)
	DeleteTemplate(ctx context.Context, key types.PoolKey, tenant string) error
	Checkpoint(ctx context.Context, id string, cred pool.Cred, name, tenant string) (types.Checkpoint, error)
	Counters() pool.Counters
	TenantClaims() map[string]int
	VolumePlacement(key types.PoolKey, tenant string, names []string) (bool, error)
	Volumes(tenant string, holders map[string]int) []types.VolumeInfo
	Sandboxes(tenant string) []pool.SandboxSummary
	Sandbox(id string) (pool.SandboxSummary, bool)
	Stats(ctx context.Context, id string) (pool.SandboxStats, bool)
	Audit(ctx context.Context, id string, line []byte)
	AuditEnabled() bool
	ClaimCheckpoint(ctx context.Context, ckptID string, ttl time.Duration, tenant string) (*types.Sandbox, error)
	ClaimCheckpointHeal(ctx context.Context, ckptID string, ttl time.Duration, tenant string) (*types.Sandbox, error)
	Checkpoints(ctx context.Context, tenant string) ([]types.Checkpoint, error)
	HasCheckpoint(ctx context.Context, ckptID string) bool
	FetchCheckpoint(ctx context.Context, ckptID string) (dir string, meta []byte, release func(), err error)
	DeleteCheckpoint(ctx context.Context, ckptID, tenant string, scope pool.DeleteScope) error
	ClaimDeadline(id, token string) (time.Time, error)
	HasGolden(ctx context.Context, key types.PoolKey, tenant string) bool
	HasPoolGolden(key types.PoolKey) bool
	HasPromotedTemplate(ctx context.Context, key types.PoolKey, tenant string) bool
	AgentSocket(id, token string) (string, error)
	DialPort(ctx context.Context, id string, cred pool.Cred, port uint16) (net.Conn, error)
	WakeAgentSocket(ctx context.Context, id, token string) (string, func(), error)
	SetPools(ctx context.Context, pools []config.PoolSpec) error
	Drain(ctx context.Context)
	Uncordon(ctx context.Context)
	Info() ([]pool.PoolInfo, pool.Gauges)
}

// Dialer opens the hybrid-vsock connection to a VM's silkd.
type Dialer interface {
	DialSilkd(ctx context.Context, vsockSocket string) (net.Conn, error)
}

// Placer names peers for redirect placement and lists the mesh; nil on a single-node deployment.
type Placer interface {
	ClientAddr(addr string) string
	Candidates(keyHash string) []string
	VolumeCandidates(keyHash string, names []string) []string
	TemplateOwners(keyHash string) []string
	VolumeOwners(names []string) []string
	TemplateVolumeOwners(keyHash string, names []string) []string
	VolumeHolders() map[string]int
	PeerAddrs() []string
	ConfigMismatches() int
}

// CheckpointProber answers which peers hold a checkpoint, asked live rather than gossiped.
type CheckpointProber interface {
	Owners(ctx context.Context, id string) []string
	Forget(id string)
}

// InfoResponse is the wire reply of GET /v1/info.
type InfoResponse struct {
	Pools            []pool.PoolInfo `json:"pools"`
	Claimed          int             `json:"claimed"`
	Hibernated       int             `json:"hibernated"`
	Archived         int             `json:"archived"`
	Draining         bool            `json:"draining,omitzero"`
	Peers            []string        `json:"peers,omitempty"`
	AtCapacity       bool            `json:"at_capacity,omitzero"`
	AtCapacityReason string          `json:"at_capacity_reason,omitempty"`
}

// SandboxListResponse is the wire reply of GET /v1/sandboxes.
type SandboxListResponse struct {
	Sandboxes []pool.SandboxSummary `json:"sandboxes"`
}

// PoolUpdateRequest is the wire body of PUT /v1/pools; omitted pools are drained.
type PoolUpdateRequest struct {
	Pools []config.PoolSpec `json:"pools"`
}

// Server serves the control plane for one node.
type Server struct {
	mgr       Manager
	dialer    Dialer
	placer    Placer
	prober    CheckpointProber
	probeKey  []byte
	apiToken  string
	tenants   []config.TenantSpec
	advertise string
	preview   *PreviewServer

	relayMu     sync.Mutex
	relays      map[net.Conn]net.Conn // client conn → guest conn
	relayClosed bool
	relayWG     sync.WaitGroup
}

// New returns a Server; an empty apiToken with no tenants leaves node-level endpoints open.
func New(apiToken string, tenants []config.TenantSpec, advertise string, mgr Manager, dialer Dialer, placer Placer, prober CheckpointProber, probeKey []byte, preview *PreviewServer) *Server {
	if host, _, err := net.SplitHostPort(advertise); err == nil {
		if ip, _ := netip.ParseAddr(host); host == "" || ip.IsUnspecified() {
			advertise = ""
		}
	}
	return &Server{
		mgr:       mgr,
		dialer:    dialer,
		placer:    placer,
		prober:    prober,
		probeKey:  probeKey,
		apiToken:  apiToken,
		tenants:   tenants,
		advertise: advertise,
		preview:   preview,
		relays:    map[net.Conn]net.Conn{},
	}
}

// Handler builds the route table.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/claim", s.requireToken(s.handleClaim))
	mux.HandleFunc("GET /v1/volumes", s.requireToken(s.handleVolumes))
	mux.HandleFunc("POST /v1/sandboxes/{id}/release", s.handleSandboxVerb("release", s.mgr.Release))
	mux.HandleFunc("POST /v1/sandboxes/{id}/hibernate", s.handleSandboxVerb("hibernate", s.mgr.Hibernate))
	mux.HandleFunc("POST /v1/sandboxes/{id}/wake", s.handleSandboxVerb("wake", s.mgr.Wake))
	mux.HandleFunc("POST /v1/sandboxes/{id}/renew", s.handleRenew)
	mux.HandleFunc("GET /v1/sandboxes/{id}", s.requireRoot(s.handleSandbox))
	mux.HandleFunc("GET /v1/sandboxes/{id}/stats", s.requireRoot(s.handleSandboxStats))
	mux.HandleFunc("POST /v1/sandboxes/{id}/fork", s.requireToken(s.handleFork))
	mux.HandleFunc("POST /v1/sandboxes/{id}/promote", s.requireToken(s.handlePromote))
	mux.HandleFunc("POST /v1/sandboxes/{id}/preview", s.requireToken(s.handlePreview))
	mux.HandleFunc("POST /v1/sandboxes/{id}/checkpoint", s.requireToken(s.handleCheckpoint))
	mux.HandleFunc("POST /v1/checkpoints/{id}/claim", s.requireToken(s.handleClaimCheckpoint))
	mux.HandleFunc("GET /v1/checkpoints", s.requireToken(s.handleListCheckpoints))
	mux.HandleFunc("GET /v1/checkpoints/{id}/blob", s.requireRoot(s.handleCheckpointBlob))
	mux.HandleFunc("HEAD /v1/checkpoints/{id}/blob", s.handleCheckpointProbe)
	mux.HandleFunc("DELETE /v1/checkpoints/{id}", s.requireToken(s.handleDeleteCheckpoint))
	mux.HandleFunc("DELETE /v1/templates", s.requireToken(s.handleDeleteTemplate))
	mux.HandleFunc("PUT /v1/pools", s.requireRoot(s.handlePutPools))
	mux.HandleFunc("POST /v1/drain", s.requireRoot(s.handleDrain))
	mux.HandleFunc("DELETE /v1/drain", s.requireRoot(s.handleUncordon))
	mux.HandleFunc("GET /v1/sandboxes/{id}/agent", s.handleAgent)
	mux.HandleFunc("GET /v1/sandboxes/{id}/ports/{port}", s.handlePort)
	mux.HandleFunc("POST /v1/sandboxes/{id}/exec", s.handleExec)
	mux.HandleFunc("GET /v1/sandboxes/{id}/owner", s.handleOwner)
	mux.HandleFunc("GET /v1/info", s.requireRoot(s.handleInfo))
	mux.HandleFunc("GET /v1/peers", s.requireToken(s.handlePeers))
	mux.HandleFunc("GET /v1/sandboxes", s.requireToken(s.handleSandboxes))
	mux.HandleFunc("GET /metrics", s.requireRoot(s.handleMetrics))
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	if s.preview != nil {
		mux.HandleFunc("/p/{token}/", s.preview.serve)
	}
	return mux
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBody[types.ClaimRequest](w, r)
	if !ok {
		return
	}
	key := req.Key()
	tenant := tenantFrom(r.Context())
	if len(req.Volumes) > 0 || req.VolumesAttachOnly {
		s.handleVolumeClaim(w, r, req, key, key.Hash(), tenant)
		return
	}

	sb, err := s.mgr.ClaimWarm(r.Context(), key, req.TTL(), tenant, req.ClaimRef, nil)
	if errors.Is(err, pool.ErrNoWarm) {
		if s.redirectClaim(r.Context(), w, req, key, key.Hash(), tenant) {
			return
		}
		sb, err = s.mgr.ClaimProvision(r.Context(), key, req.TTL(), tenant, req.ClaimRef, nil)
	}
	if errors.Is(err, pool.ErrQuota) && s.placer != nil && !req.NoRedirect &&
		s.writeRedirect(w, s.placer.Candidates(key.Hash())) {
		return
	}
	writeResult(w, r, "claim", key.Template, "provisioning failed", err, func() {
		writeJSON(w, http.StatusOK, s.claimResponse(sb))
	})
}

func (s *Server) handleVolumeClaim(w http.ResponseWriter, r *http.Request, req types.ClaimRequest, key types.PoolKey, hash, tenant string) {
	volumes, err := types.ValidateVolumes(req.Volumes, req.VolumesAttachOnly)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf("%v: %v", pool.ErrBadVolume, err))
		return
	}
	req.Volumes = volumes
	redirected, err := s.redirectVolumeClaim(r.Context(), w, &req, key, hash, tenant)
	if err != nil {
		writeResult(w, r, "claim", key.Template, "provisioning failed", err, func() {})
		return
	}
	if redirected {
		return
	}
	var sb *types.Sandbox
	if req.RequirePromoted {
		sb, err = s.mgr.ClaimProvisionPromoted(r.Context(), key, req.TTL(), tenant, req.ClaimRef, req.Volumes)
	} else {
		sb, err = s.mgr.ClaimWarm(r.Context(), key, req.TTL(), tenant, req.ClaimRef, req.Volumes)
		if errors.Is(err, pool.ErrNoWarm) {
			if s.placer != nil && !req.NoRedirect && s.writeRedirect(w, s.placer.VolumeCandidates(hash, types.VolumeNames(req.Volumes))) {
				return
			}
			sb, err = s.mgr.ClaimProvision(r.Context(), key, req.TTL(), tenant, req.ClaimRef, req.Volumes)
		}
	}
	writeResult(w, r, "claim", key.Template, "provisioning failed", err, func() {
		writeJSON(w, http.StatusOK, s.claimResponse(sb))
	})
}

func (s *Server) handleVolumes(w http.ResponseWriter, r *http.Request) {
	var holders map[string]int
	if s.placer != nil {
		holders = s.placer.VolumeHolders()
	}
	writeJSON(w, http.StatusOK, types.VolumeListResponse{
		Volumes: s.mgr.Volumes(tenantFrom(r.Context()), holders),
	})
}

func (s *Server) redirectVolumeClaim(ctx context.Context, w http.ResponseWriter, req *types.ClaimRequest, key types.PoolKey, hash, tenant string) (bool, error) {
	names := types.VolumeNames(req.Volumes)
	localVolumes, err := s.mgr.VolumePlacement(key, tenant, names)
	if err != nil {
		return false, err
	}

	localTemplate := s.mgr.HasPromotedTemplate(ctx, key, tenant)
	pooled := s.mgr.HasPoolGolden(key)
	var templateOwners []string
	if s.placer != nil && !pooled {
		templateOwners = s.templateOwners(s.placer.TemplateOwners, hash, tenant)
	}
	promoted := req.RequirePromoted || localTemplate || len(templateOwners) > 0
	req.RequirePromoted = promoted
	if localVolumes && (!promoted || localTemplate || pooled) {
		return false, nil
	}
	if s.placer == nil || req.NoRedirect {
		return false, pool.ErrVolumeUnavailable
	}

	var owners []string
	if promoted {
		owners = s.templateOwners(func(probe string) []string {
			return s.placer.TemplateVolumeOwners(probe, names)
		}, hash, tenant)
	} else {
		owners = s.placer.VolumeCandidates(hash, names)
	}
	if len(owners) == 0 {
		owners = s.placer.VolumeOwners(names)
	}
	if len(owners) == 0 {
		return false, pool.ErrVolumeUnavailable
	}
	writeJSON(w, http.StatusOK, types.ClaimResponse{Redirect: s.clientAddrs(owners), RequirePromoted: promoted})
	return true, nil
}

func (s *Server) templateOwners(query func(string) []string, hash, tenant string) []string {
	probes := []string{types.TemplateGossipHash(hash, tenant)}
	if tenant == "" {
		for _, tn := range s.tenants {
			probes = append(probes, types.TemplateGossipHash(hash, tn.Name))
		}
	} else {
		probes = append(probes, types.TemplateGossipHash(hash, ""))
	}
	var owners []string
	for _, probe := range probes {
		for _, owner := range query(probe) {
			if !slices.Contains(owners, owner) {
				owners = append(owners, owner)
			}
		}
	}
	return owners
}

func (s *Server) redirectClaim(ctx context.Context, w http.ResponseWriter, req types.ClaimRequest, key types.PoolKey, hash, tenant string) bool {
	if s.placer == nil || req.NoRedirect {
		return false
	}
	if s.writeRedirect(w, s.placer.Candidates(hash)) {
		return true
	}
	owners := s.templateOwners(s.placer.TemplateOwners, hash, tenant)
	return len(owners) > 0 && !s.mgr.HasGolden(ctx, key, tenant) && s.writeRedirect(w, owners)
}

func (s *Server) handleSandbox(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sb, ok := s.mgr.Sandbox(id)
	if !ok {
		writePoolErr(w, pool.ErrUnknownSandbox)
		return
	}
	writeJSON(w, http.StatusOK, sb)
}

// handleSandboxes lists the live claims visible to the caller — never tokens.
func (s *Server) handleSandboxes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, SandboxListResponse{Sandboxes: s.mgr.Sandboxes(tenantFrom(r.Context()))})
}

func (s *Server) handleSandboxStats(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	st, ok := s.mgr.Stats(r.Context(), id)
	if !ok {
		writePoolErr(w, pool.ErrUnknownSandbox)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleSandboxVerb(verb string, do func(ctx context.Context, id string, cred pool.Cred) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := sandboxToken(w, r)
		if !ok {
			return
		}
		id := r.PathValue("id")
		err := do(r.Context(), id, s.sandboxCred(token))
		writeResult(w, r, verb, id, verb+" failed", err, func() {
			w.WriteHeader(http.StatusNoContent)
		})
	}
}

func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	token, ok := sandboxToken(w, r)
	if !ok {
		return
	}
	req, ok := decodeBody[types.RenewRequest](w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	deadline, err := s.mgr.Renew(r.Context(), id, s.sandboxCred(token), req.TTL())
	writeResult(w, r, "renew", id, "renew failed", err, func() {
		writeJSON(w, http.StatusOK, types.RenewResponse{Deadline: deadline})
	})
}

func (s *Server) handleFork(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBody[types.ForkRequest](w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	children, err := s.mgr.Fork(r.Context(), id, s.bodyCred(r, req.Token), req.Count, req.TTL())
	writeResult(w, r, "fork", id, "fork failed", err, func() {
		resp := types.ForkResponse{Children: make([]types.ClaimResponse, len(children))}
		for i, c := range children {
			resp.Children[i] = s.claimResponse(c)
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

func (s *Server) handlePromote(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBody[types.PromoteRequest](w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	key, digest, err := s.mgr.Promote(r.Context(), id, s.bodyCred(r, req.Token), req.Template, tenantFrom(r.Context()))
	writeResult(w, r, "promote", id, "promote failed", err, func() {
		writeJSON(w, http.StatusOK, types.PromoteResponse{Key: key, ContentDigest: digest})
	})
}

func (s *Server) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBody[types.CheckpointRequest](w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	ckpt, err := s.mgr.Checkpoint(r.Context(), id, s.bodyCred(r, req.Token), req.Name, tenantFrom(r.Context()))
	writeResult(w, r, "checkpoint", id, "checkpoint failed", err, func() {
		writeJSON(w, http.StatusOK, types.CheckpointResponse{Checkpoint: ckpt})
	})
}

func (s *Server) handleClaimCheckpoint(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBody[types.CheckpointClaimRequest](w, r)
	if !ok {
		return
	}
	ckptID := r.PathValue("id")
	sb, err := s.mgr.ClaimCheckpoint(r.Context(), ckptID, req.TTL(), tenantFrom(r.Context()))
	if errors.Is(err, pool.ErrUnknownCheckpoint) {
		if !req.NoRedirect && s.prober != nil && s.writeRedirect(w, s.prober.Owners(r.Context(), ckptID)) {
			return
		}
		sb, err = s.mgr.ClaimCheckpointHeal(r.Context(), ckptID, req.TTL(), tenantFrom(r.Context()))
	}
	writeResult(w, r, "claim checkpoint", ckptID, "provisioning failed", err, func() {
		writeJSON(w, http.StatusOK, s.claimResponse(sb))
	})
}

func (s *Server) handleCheckpointBlob(w http.ResponseWriter, r *http.Request) {
	ckptID := r.PathValue("id")
	dir, meta, release, err := s.mgr.FetchCheckpoint(r.Context(), ckptID)
	writeResult(w, r, "fetch checkpoint", ckptID, "fetch checkpoint failed", err, func() {
		defer release()
		w.Header().Set("Content-Type", "application/x-tar")
		w.WriteHeader(http.StatusOK)
		if err := peer.TarRecord(dir, meta, w); err != nil {
			log.WithFunc("server.handleCheckpointBlob").Error(r.Context(), err, "stream checkpoint")
		}
	})
}

func (s *Server) handleCheckpointProbe(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if len(s.probeKey) > 0 && !peer.VerifyProbeMAC(s.probeKey, id, r.Header.Get(peer.ProbeHeader)) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if s.mgr.HasCheckpoint(r.Context(), id) {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (s *Server) handleListCheckpoints(w http.ResponseWriter, r *http.Request) {
	ckpts, err := s.mgr.Checkpoints(r.Context(), tenantFrom(r.Context()))
	if err != nil {
		log.WithFunc("server.handleListCheckpoints").Error(r.Context(), err, "list checkpoints")
		writeErr(w, http.StatusInternalServerError, "list checkpoints failed")
		return
	}
	writeJSON(w, http.StatusOK, types.CheckpointListResponse{Checkpoints: ckpts})
}

func (s *Server) handleDeleteCheckpoint(w http.ResponseWriter, r *http.Request) {
	scope := pool.DeleteFleet
	if r.URL.Query().Get("no_forward") != "" {
		scope = pool.DeleteLocal
	}
	id := r.PathValue("id")
	err := s.mgr.DeleteCheckpoint(r.Context(), id, tenantFrom(r.Context()), scope)
	if s.prober != nil {
		s.prober.Forget(id)
	}
	writeResult(w, r, "delete checkpoint", id, "delete checkpoint failed", err, func() {
		w.WriteHeader(http.StatusNoContent)
	})
}

func (s *Server) handleDeleteTemplate(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	key := types.PoolKey{Template: q.Get("template"), Net: types.NetShape(q.Get("net")), Size: types.Size(q.Get("size"))}.Defaulted()
	err := s.mgr.DeleteTemplate(r.Context(), key, tenantFrom(r.Context()))
	if errors.Is(err, pool.ErrUnknownTemplate) && s.placer != nil && q.Get("no_redirect") == "" &&
		s.writeRedirect(w, s.templateOwners(s.placer.TemplateOwners, key.Hash(), tenantFrom(r.Context()))) {
		return
	}
	writeResult(w, r, "delete template", key.Template, "delete template failed", err, func() {
		w.WriteHeader(http.StatusNoContent)
	})
}

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

func (s *Server) handlePeers(w http.ResponseWriter, _ *http.Request) {
	var peers []string
	if s.placer != nil {
		peers = s.placer.PeerAddrs()
	}
	writeJSON(w, http.StatusOK, map[string][]string{"peers": s.clientAddrs(peers)})
}

func (s *Server) handleInfo(w http.ResponseWriter, _ *http.Request) {
	pools, g := s.mgr.Info()
	resp := InfoResponse{
		Pools: pools, Claimed: g.Claimed, Hibernated: g.Hibernated, Archived: g.Archived,
		Draining: g.Draining, AtCapacity: g.AtCapacity, AtCapacityReason: g.AtCapacityReason,
	}
	if s.placer != nil {
		resp.Peers = s.clientAddrs(s.placer.PeerAddrs())
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = io.WriteString(w, "ok")
}

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

func (s *Server) requireRoot(next http.HandlerFunc) http.HandlerFunc {
	return s.requireToken(func(w http.ResponseWriter, r *http.Request) {
		if tenantFrom(r.Context()) != "" {
			writeErr(w, http.StatusForbidden, "operator token required")
			return
		}
		next(w, r)
	})
}

func (s *Server) rootRequest(r *http.Request) bool {
	token, ok := bearerToken(r)
	return ok && s.isRootToken(token)
}

func (s *Server) isRootToken(token string) bool {
	return s.apiToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.apiToken)) == 1
}

func (s *Server) sandboxCred(token string) pool.Cred {
	if s.isRootToken(token) {
		return pool.Cred{Operator: true}
	}
	return pool.Cred{Token: token}
}

func (s *Server) bodyCred(r *http.Request, bodyToken string) pool.Cred {
	return pool.Cred{Token: bodyToken, Operator: bodyToken == "" && s.rootRequest(r)}
}

func (s *Server) resolveScope(r *http.Request) (string, bool) {
	if s.apiToken == "" && len(s.tenants) == 0 {
		return "", true
	}
	token, ok := bearerToken(r)
	if !ok {
		return "", false
	}
	if s.isRootToken(token) {
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
		OwnerAddr: s.advertise, FromCheckpoint: sb.FromCheckpoint, TemplateDigest: sb.TemplateDigest,
		Volumes: sb.Volumes,
	}
}
