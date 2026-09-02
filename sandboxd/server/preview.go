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
	"strconv"
	"strings"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type previewClaims struct {
	ID    string `json:"id"`
	Port  uint16 `json:"port"`
	Owner string `json:"owner"`
	Exp   int64  `json:"exp"`
}

// PreviewManager is the slice of the pool manager the preview path needs.
type PreviewManager interface {
	PreviewDial(ctx context.Context, id string, port uint16) (net.Conn, error)
}

// PreviewServer serves signed guest HTTP URLs and forwards requests to their owner node.
type PreviewServer struct {
	secret    []byte
	base      string
	owner     string
	mgr       PreviewManager
	transport *http.Transport
}

// NewPreviewServer returns nil when preview is not configured.
func NewPreviewServer(secret, base, owner string, mgr PreviewManager) *PreviewServer {
	if secret == "" {
		return nil
	}
	p := &PreviewServer{secret: []byte(secret), base: base, owner: owner, mgr: mgr}
	p.transport = &http.Transport{
		// a pooled guest conn skips PreviewDial, and with it wakeResolved and the Transition lock
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			id, portStr, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			port, err := strconv.ParseUint(portStr, 10, 16)
			if err != nil {
				return nil, err
			}
			return p.mgr.PreviewDial(ctx, id, uint16(port))
		},
	}
	return p
}

// Mint returns a preview URL for a guest port, valid for ttl.
func (p *PreviewServer) Mint(id string, port uint16, ttl time.Duration) string {
	claims := previewClaims{ID: id, Port: port, Owner: p.owner, Exp: time.Now().Add(ttl).Unix()}
	payload, _ := json.Marshal(claims)
	enc := base64.RawURLEncoding.EncodeToString(payload)
	token := enc + "." + p.sign(enc)
	base := p.base
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	return fmt.Sprintf("%s/p/%s/", strings.TrimRight(base, "/"), token)
}

// Handler serves preview requests and health checks on preview_listen.
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
	if claims.Owner != p.owner {
		p.forward(w, r, claims.Owner)
		return
	}
	p.proxyLocal(w, r, claims)
}

func (p *PreviewServer) proxyLocal(w http.ResponseWriter, r *http.Request, claims previewClaims) {
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = "http"
			// the synthetic host carries PreviewDial's target
			pr.Out.URL.Host = fmt.Sprintf("%s:%d", claims.ID, claims.Port)
			pr.Out.URL.Path = "/" + strings.TrimPrefix(pr.In.URL.Path, "/p/"+r.PathValue("token")+"/")
			// browser credentials for the preview domain must not reach guest code
			pr.Out.Header.Del("Cookie")
			pr.Out.Header.Del("Authorization")
		},
		Transport: p.transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.WithFunc("server.proxyLocal").Errorf(r.Context(), err, "proxy %s:%d", claims.ID, claims.Port)
			http.Error(w, "preview target unreachable", http.StatusBadGateway)
		},
	}
	rp.ServeHTTP(w, r) //nolint:gosec // target derived from an HMAC-signed token, not client input
}

func (p *PreviewServer) forward(w http.ResponseWriter, r *http.Request, owner string) {
	target := &url.URL{Scheme: "http", Host: owner}
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.WithFunc("server.forward").Errorf(r.Context(), err, "forward to owner %s", owner)
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

func (s *Server) handlePreview(w http.ResponseWriter, r *http.Request) {
	if s.preview == nil {
		writeErr(w, http.StatusNotImplemented, "preview not configured")
		return
	}
	req, ok := decodeBody[types.PreviewRequest](w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	deadline, err := s.mgr.ClaimDeadline(id, req.Token)
	if err != nil {
		writePoolErr(w, err)
		return
	}
	ttl := req.TTL()
	switch {
	case !deadline.IsZero():
		if lease := time.Until(deadline); ttl <= 0 || ttl > lease {
			ttl = lease // never outlive the claim
		}
	case ttl <= 0:
		ttl = previewTTL
	}
	writeJSON(w, http.StatusOK, types.PreviewResponse{URL: s.preview.Mint(id, req.Port, ttl)})
}
