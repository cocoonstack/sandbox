package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/projecteru2/core/log"
)

// previewClaims is the signed payload of a preview URL: it authorizes serving
// guest `Port` of sandbox `ID`, owned by the node reachable at preview base
// `Owner`, until `Exp`. Signed so any node can verify without shared state.
type previewClaims struct {
	ID    string `json:"id"`
	Port  uint16 `json:"port"`
	Owner string `json:"owner"`
	Exp   int64  `json:"exp"`
}

// PreviewServer serves guest HTTP apps under signed, expiring URLs, built on
// the port relay. The whole mechanism lives here: a signed token needs no
// revocation list (a released sandbox simply isn't in the claim map), and
// because the token carries its owner's preview address, any node can accept
// a request and proxy it to the owner — so the public entry point is a dumb
// TLS proxy, not a stateful gateway.
type PreviewServer struct {
	secret    []byte
	advertise string
	mgr       PreviewManager
}

// PreviewManager is the slice of the pool manager the preview path needs.
type PreviewManager interface {
	PreviewDial(ctx context.Context, id string, port uint16) (net.Conn, error)
}

// NewPreviewServer returns nil when preview is not configured (empty secret).
// advertise is this node's preview base (host:port a browser/proxy reaches).
func NewPreviewServer(secret, advertise string, mgr PreviewManager) *PreviewServer {
	if secret == "" {
		return nil
	}
	return &PreviewServer{secret: []byte(secret), advertise: advertise, mgr: mgr}
}

// Mint returns a preview URL for a guest port, valid for ttl (bounded by the
// caller to the claim's deadline). Called on the owner node with the sandbox
// already authorized, so the token names this node as owner.
func (p *PreviewServer) Mint(id string, port uint16, ttl time.Duration) string {
	claims := previewClaims{ID: id, Port: port, Owner: p.advertise, Exp: time.Now().Add(ttl).Unix()}
	payload, _ := json.Marshal(claims)
	enc := base64.RawURLEncoding.EncodeToString(payload)
	token := enc + "." + p.sign(enc)
	base := p.advertise
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	return fmt.Sprintf("%s/p/%s/", strings.TrimRight(base, "/"), token)
}

// Handler serves the preview_listen address: verify the token, then either
// reverse-proxy to the guest port locally or forward to the owner node.
func (p *PreviewServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/p/{token}/", p.serve)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	return mux
}

func (p *PreviewServer) serve(w http.ResponseWriter, r *http.Request) {
	claims, ok := p.verify(r.PathValue("token"))
	if !ok {
		http.Error(w, "invalid or expired preview token", http.StatusForbidden)
		return
	}
	if claims.Owner != p.advertise {
		p.forward(w, r, claims.Owner)
		return
	}
	p.proxyLocal(w, r, claims)
}

// proxyLocal reverse-proxies to the guest port over a relay connection whose
// liveness lookup is the revocation check: a released sandbox is gone from
// the claim map, so PreviewDial fails and the URL 404s.
func (p *PreviewServer) proxyLocal(w http.ResponseWriter, r *http.Request, claims previewClaims) {
	rp := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "http"
			req.URL.Host = "guest"
			req.URL.Path = "/" + strings.TrimPrefix(req.URL.Path, "/p/"+r.PathValue("token")+"/")
			// The preview URL is bearer-authorized by its token; never hand
			// the browser's ambient credentials for the preview domain to
			// untrusted guest code.
			req.Header.Del("Cookie")
			req.Header.Del("Authorization")
		},
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return p.mgr.PreviewDial(ctx, claims.ID, claims.Port)
			},
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			log.WithFunc("server.preview").Errorf(context.Background(), err, "proxy %s:%d", claims.ID, claims.Port)
			http.Error(w, "preview target unreachable", http.StatusBadGateway)
		},
	}
	rp.ServeHTTP(w, r) //nolint:gosec // target derived from an HMAC-signed token, not client input
}

// forward relays the request to the owner node's preview_listen — the token
// self-authorizes, so this node is a dumb hop.
func (p *PreviewServer) forward(w http.ResponseWriter, r *http.Request, owner string) {
	target := &url.URL{Scheme: "http", Host: owner}
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		log.WithFunc("server.preview").Errorf(context.Background(), err, "forward to owner %s", owner)
		http.Error(w, "owner node unreachable", http.StatusBadGateway)
	}
	rp.ServeHTTP(w, r) //nolint:gosec // owner host comes from an HMAC-signed token
}

func (p *PreviewServer) sign(enc string) string {
	mac := hmac.New(sha256.New, p.secret)
	_, _ = mac.Write([]byte(enc))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (p *PreviewServer) verify(token string) (previewClaims, bool) {
	enc, sig, found := strings.Cut(token, ".")
	if !found || !hmac.Equal([]byte(sig), []byte(p.sign(enc))) {
		return previewClaims{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(enc)
	if err != nil {
		return previewClaims{}, false
	}
	var claims previewClaims
	if err := json.Unmarshal(payload, &claims); err != nil || time.Now().Unix() > claims.Exp {
		return previewClaims{}, false
	}
	return claims, true
}

// PreviewRequest is the wire body of POST /v1/sandboxes/{id}/preview.
type PreviewRequest struct {
	Token      string `json:"token"`
	Port       uint16 `json:"port"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}

// PreviewResponse carries the minted URL.
type PreviewResponse struct {
	URL string `json:"url"`
}

// handlePreview mints a preview URL for a claimed sandbox's port. The sandbox
// token authorizes it; the TTL is clamped to the claim's remaining lease.
func (s *Server) handlePreview(w http.ResponseWriter, r *http.Request) {
	if s.preview == nil {
		writeErr(w, http.StatusNotImplemented, "preview not configured")
		return
	}
	req, ok := decodeBody[PreviewRequest](w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	deadline, err := s.mgr.ClaimDeadline(id, req.Token)
	if err != nil {
		writePoolErr(w, err)
		return
	}
	ttl := time.Duration(req.TTLSeconds) * time.Second
	if lease := time.Until(deadline); ttl <= 0 || ttl > lease {
		ttl = lease // never outlive the claim
	}
	writeJSON(w, http.StatusOK, PreviewResponse{URL: s.preview.Mint(id, req.Port, ttl)})
}
