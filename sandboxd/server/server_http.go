package server

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const retryAfterSeconds = "10"

type tenantKey struct{}

func (s *Server) writeRedirect(w http.ResponseWriter, addrs []string) bool {
	if len(addrs) == 0 {
		return false
	}
	writeJSON(w, http.StatusOK, ClaimResponse{Redirect: s.clientAddrs(addrs)})
	return true
}

func (s *Server) clientAddrs(addrs []string) []string {
	if s.placer == nil {
		return addrs
	}
	clients := make([]string, len(addrs))
	for i, addr := range addrs {
		clients[i] = s.placer.ClientAddr(addr)
	}
	return clients
}

func withTenant(ctx context.Context, tenant *tenants.TenantRecord) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenant)
}

func tenantFrom(ctx context.Context) string {
	if tenant := tenantRecordFrom(ctx); tenant != nil {
		return tenant.Name
	}
	return ""
}

// tenantRecordFrom is nil for root.
func tenantRecordFrom(ctx context.Context) *tenants.TenantRecord {
	tenant, _ := ctx.Value(tenantKey{}).(*tenants.TenantRecord)
	return tenant
}

func bearerToken(r *http.Request) (string, bool) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return token, ok && token != ""
}

func sandboxToken(w http.ResponseWriter, r *http.Request) (string, bool) {
	token, ok := bearerToken(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "missing bearer token")
	}
	return token, ok
}

// upgradeGate refuses a bare GET: waking the sandbox consumes its hibernate snapshot.
func upgradeGate(w http.ResponseWriter, r *http.Request, proto string) (string, bool) {
	token, ok := sandboxToken(w, r)
	if !ok {
		return "", false
	}
	if !strings.EqualFold(r.Header.Get("Upgrade"), proto) {
		w.Header().Set("Upgrade", proto)
		writeErr(w, http.StatusUpgradeRequired, "upgrade to "+proto+" required")
		return "", false
	}
	return token, true
}

// hijackClient on failure has already closed guest and answered the request.
func hijackClient(ctx context.Context, w http.ResponseWriter, guest net.Conn) (net.Conn, *bufio.Reader, bool) {
	client, buf, ok := hijack(ctx, w)
	if !ok {
		_ = guest.Close()
	}
	return client, buf, ok
}

func hijack(ctx context.Context, w http.ResponseWriter) (net.Conn, *bufio.Reader, bool) {
	client, bufrw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		log.WithFunc("server.hijack").Error(ctx, err, "hijack")
		writeErr(w, http.StatusInternalServerError, "connection cannot be hijacked")
		return nil, nil, false
	}
	return client, bufrw.Reader, true
}

func decodeBody[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var v T
	if err := json.UnmarshalRead(http.MaxBytesReader(w, r.Body, maxBodyBytes), &v, json.MatchCaseInsensitiveNames(true)); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return v, false
	}
	return v, true
}

// decodeOptionalBody reads an absent body as the zero value, for verbs whose fields all default.
func decodeOptionalBody[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var v T
	err := json.UnmarshalDecode(jsontext.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)), &v, json.MatchCaseInsensitiveNames(true))
	if errors.Is(err, io.EOF) {
		return v, true
	}
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return v, false
	}
	return v, true
}

func decodeBodyStrict[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var v T
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return v, false
	}
	if err := utils.DecodeStrictJSON(raw, &v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return v, false
	}
	return v, true
}

func validRequest(w http.ResponseWriter, errs ...error) bool {
	if err := errors.Join(errs...); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return false
	}
	return true
}

func metadataFilter(pairs []string) (types.Metadata, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	filter := make(types.Metadata, len(pairs))
	for _, pair := range pairs {
		k, v, ok := strings.Cut(pair, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("metadata filter %q must be key=value", pair)
		}
		if _, dup := filter[k]; dup {
			return nil, fmt.Errorf("metadata filter repeats key %q", k)
		}
		filter[k] = v
	}
	return filter, nil
}

func writePoolErr(w http.ResponseWriter, err error) bool {
	if volumeErr, ok := errors.AsType[*pool.VolumeError](err); ok {
		if volumeErr.Status == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", retryAfterSeconds)
		}
		writeJSON(w, volumeErr.Status, volumeErr)
		return true
	}
	code, msg, ok := mapPoolErr(w, err)
	if ok {
		writeErr(w, code, msg)
	}
	return ok
}

func mapPoolErr(w http.ResponseWriter, err error) (code int, msg string, ok bool) {
	code, msg, ok = lookupPoolErr(err)
	if ok && code == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", retryAfterSeconds)
	}
	return code, msg, ok
}

func lookupPoolErr(err error) (code int, msg string, ok bool) {
	if volumeErr, ok := errors.AsType[*pool.VolumeError](err); ok {
		return volumeErr.Status, volumeErr.Message, true
	}
	for _, m := range poolErrHTTP {
		if errors.Is(err, m.err) {
			return m.code, cmp.Or(m.msg, err.Error()), true
		}
	}
	return 0, "", false
}

func writeResult(w http.ResponseWriter, r *http.Request, op, id, failMsg string, err error, ok func()) {
	switch {
	case writePoolErr(w, err):
	case err != nil:
		log.WithFunc("server.writeResult").Errorf(r.Context(), err, "%s %s", op, id)
		writeErr(w, http.StatusInternalServerError, failMsg)
	default:
		ok()
	}
}

func noContent(w http.ResponseWriter) func() {
	return func() { w.WriteHeader(http.StatusNoContent) }
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.MarshalWrite(w, v, jsontext.AllowInvalidUTF8(true))
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
