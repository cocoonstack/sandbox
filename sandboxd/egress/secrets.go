package egress

import (
	"fmt"
	"os"
)

// SecretSpec declares a node-side credential the proxy injects: Header is the
// request header it sets, valued from ValueEnv (an env var, so the literal
// never sits in the config file) or Value.
type SecretSpec struct {
	Name     string `json:"name"`
	Header   string `json:"header"`
	Value    string `json:"value,omitempty"`
	ValueEnv string `json:"value_env,omitempty"` //nolint:gosec // env var name, not a value
}

func (s SecretSpec) Validate() error {
	switch {
	case s.Name == "":
		return fmt.Errorf("secret name must not be empty")
	case s.Header == "":
		return fmt.Errorf("secret %q: header must not be empty", s.Name)
	case s.Value == "" && s.ValueEnv == "":
		return fmt.Errorf("secret %q: needs value or value_env", s.Name)
	}
	return nil
}

// SecretStore is the resolved node-side credential registry, implementing the
// Proxy's Secrets interface. Values live only here — never in a policy or a
// gossiped struct.
type SecretStore struct {
	byName map[string]resolvedSecret
}

// NewSecretStore resolves each spec's value; an unset ValueEnv is an error,
// never a silently-empty credential.
func NewSecretStore(specs []SecretSpec) (*SecretStore, error) {
	byName := make(map[string]resolvedSecret, len(specs))
	for _, s := range specs {
		value := s.Value
		if s.ValueEnv != "" {
			v, ok := os.LookupEnv(s.ValueEnv)
			if !ok {
				return nil, fmt.Errorf("secret %q: env %s is not set", s.Name, s.ValueEnv)
			}
			value = v
		}
		byName[s.Name] = resolvedSecret{header: s.Header, value: value}
	}
	return &SecretStore{byName: byName}, nil
}

func (s *SecretStore) Header(name string) (header, value string, ok bool) {
	r, ok := s.byName[name]
	return r.header, r.value, ok
}

type resolvedSecret struct {
	header string
	value  string
}
