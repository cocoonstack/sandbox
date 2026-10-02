package egress

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"golang.org/x/net/http/httpguts"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// nonInjectable lists the secret headers the transport owns or strips.
var nonInjectable = slices.Concat([]string{"Host", "Content-Length"}, hopHeaders)

// SecretSpec declares a credential the proxy injects, valued from the env ValueEnv names, or Name when that is empty.
type SecretSpec struct {
	Name     string  `json:"name"`
	Header   string  `json:"header"`
	Value    *string `json:"value,omitempty"` // rejected whenever present: a value never sits in the config file
	ValueEnv string  `json:"value_env"`
}

func (s SecretSpec) Validate() error {
	switch {
	case s.Name == "":
		return fmt.Errorf("secret name must not be empty")
	case s.Value != nil:
		return fmt.Errorf("secret %q: value is not supported, use value_env", s.Name)
	case s.ValueEnv == "" && !types.EnvNameRe.MatchString(s.Name):
		return fmt.Errorf("secret %q: without value_env the name is the claim env to read, so it must match %s", s.Name, types.EnvNameRe)
	}
	if err := CheckInjectHeader(s.Header); err != nil {
		return fmt.Errorf("secret %q: %w", s.Name, err)
	}
	return nil
}

type resolvedSecret struct {
	header string
	env    string
	value  string
}

// SecretStore is the resolved node-side credential registry; values live only here.
type SecretStore struct {
	byName map[string]resolvedSecret
}

// NewSecretStore resolves each spec's value; a named env that is unset or invalid is an error.
func NewSecretStore(specs []SecretSpec) (*SecretStore, error) {
	byName := make(map[string]resolvedSecret, len(specs))
	for _, s := range specs {
		if s.ValueEnv == "" {
			byName[s.Name] = resolvedSecret{header: s.Header, env: s.Name}
			continue
		}
		v := os.Getenv(s.ValueEnv)
		if v == "" {
			return nil, fmt.Errorf("secret %q: env %s is unset or empty", s.Name, s.ValueEnv)
		}
		if !httpguts.ValidHeaderFieldValue(v) {
			return nil, fmt.Errorf("secret %q: env %s holds an invalid header value", s.Name, s.ValueEnv)
		}
		byName[s.Name] = resolvedSecret{header: s.Header, env: s.ValueEnv, value: v}
	}
	return &SecretStore{byName: byName}, nil
}

// Header resolves a declared secret to its node value, empty for one only a claim supplies.
func (s *SecretStore) Header(name string) (header, value string, ok bool) {
	r, ok := s.byName[name]
	return r.header, r.value, ok
}

// EnvName names the env a claim sets to supply the secret's value.
func (s *SecretStore) EnvName(name string) string {
	return s.byName[name].env
}

// CheckInjectHeader rejects a header name the proxy cannot set: an invalid one, or one the transport owns or strips.
func CheckInjectHeader(header string) error {
	switch {
	case !httpguts.ValidHeaderFieldName(header):
		return fmt.Errorf("header %q is not a valid header name", header)
	case slices.ContainsFunc(nonInjectable, func(h string) bool { return strings.EqualFold(h, header) }):
		return fmt.Errorf("header %s is not injectable", header)
	}
	return nil
}
