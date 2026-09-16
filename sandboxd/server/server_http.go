package server

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const retryAfterSeconds = "10"

type tenantKey struct{}

func (s *Server) writeRedirect(w http.ResponseWriter, addrs []string) bool {
	if len(addrs) == 0 {
		return false
	}
	writeJSON(w, http.StatusOK, types.ClaimResponse{Redirect: s.clientAddrs(addrs)})
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

func withTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenant)
}

func tenantFrom(ctx context.Context) string {
	tenant, _ := ctx.Value(tenantKey{}).(string)
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

func decodeBody[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var v T
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&v); err != nil {
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

func writePoolErr(w http.ResponseWriter, err error) bool {
	for _, m := range poolErrHTTP {
		if errors.Is(err, m.err) {
			if m.code == http.StatusServiceUnavailable {
				w.Header().Set("Retry-After", retryAfterSeconds)
			}
			writeErr(w, m.code, cmp.Or(m.msg, err.Error()))
			return true
		}
	}
	return false
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

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
