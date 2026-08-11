package egress

import (
	"fmt"
	"os"
	"strings"

	"golang.org/x/net/http/httpguts"
)

// nonInjectable rejects secret headers the transport owns or strips —
// injecting one would audit as done while the request carries something
// else. Derived from the proxy's hop set so the two lists cannot drift.
var nonInjectable = func() map[string]struct{} {
	m := map[string]struct{}{"host": {}, "content-length": {}}
	for _, h := range hopHeaders {
		m[strings.ToLower(h)] = struct{}{}
	}
	return m
}()

// SecretSpec declares a node-side credential the proxy injects: Header is the
// request header it sets, valued from the ValueEnv env var — the literal
// never sits in the config file.
type SecretSpec struct {
	Name     string  `json:"name"`
	Header   string  `json:"header"`
	Value    *string `json:"value,omitempty"` // rejected whenever present — value_env is the only source
	ValueEnv string  `json:"value_env"`       //nolint:gosec // env var name, not a value
}

func (s SecretSpec) Validate() error {
	switch {
	case s.Name == "":
		return fmt.Errorf("secret name must not be empty")
	case s.Value != nil:
		return fmt.Errorf("secret %q: value is not supported, use value_env", s.Name)
	case !httpguts.ValidHeaderFieldName(s.Header):
		return fmt.Errorf("secret %q: header %q is not a valid header name", s.Name, s.Header)
	case s.ValueEnv == "":
		return fmt.Errorf("secret %q: needs value_env", s.Name)
	}
	if _, bad := nonInjectable[strings.ToLower(s.Header)]; bad {
		return fmt.Errorf("secret %q: header %s is not injectable", s.Name, s.Header)
	}
	return nil
}

type resolvedSecret struct {
	header string
	value  string
}

// SecretStore is the resolved node-side credential registry, implementing the
// Proxy's Secrets interface. Values live only here — never in a policy or a
// gossiped struct.
type SecretStore struct {
	byName map[string]resolvedSecret
}

// NewSecretStore resolves each spec's value; an unset or empty ValueEnv is an
// error, never a silently-empty credential, and a value a header cannot carry
// (CTLs would split the injected request) is rejected before it can be sent.
func NewSecretStore(specs []SecretSpec) (*SecretStore, error) {
	byName := make(map[string]resolvedSecret, len(specs))
	for _, s := range specs {
		v := os.Getenv(s.ValueEnv)
		if v == "" {
			return nil, fmt.Errorf("secret %q: env %s is unset or empty", s.Name, s.ValueEnv)
		}
		if !httpguts.ValidHeaderFieldValue(v) {
			return nil, fmt.Errorf("secret %q: env %s holds an invalid header value", s.Name, s.ValueEnv)
		}
		byName[s.Name] = resolvedSecret{header: s.Header, value: v}
	}
	return &SecretStore{byName: byName}, nil
}

func (s *SecretStore) Header(name string) (header, value string, ok bool) {
	r, ok := s.byName[name]
	return r.header, r.value, ok
}
