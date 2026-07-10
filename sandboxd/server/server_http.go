package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

// tenantKey carries the resolved tenant scope ("" = root) on the request
// context, from requireToken to the handlers.
type tenantKey struct{}

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

// decodeBodyStrict is decodeBody for operator bodies, where a mistyped or
// repeated key must fail instead of silently no-opping; client-facing bodies
// stay lenient (newer SDK fields must not break older nodes).
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
