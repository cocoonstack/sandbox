package types

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"golang.org/x/net/http/httpguts"
)

const (
	maxEnvVars       = 64
	maxEnvValueBytes = 8 << 10
	maxEnvBytes      = 64 << 10
	maxInjectHosts   = 8
	maxPlaceholder   = 256
)

var (
	// EnvNameRe is the portable shell variable name.
	EnvNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
	// injectHostRe is a lowercase DNS name or IPv4 literal, optionally under a "*." wildcard.
	injectHostRe = regexp.MustCompile(`^(\*\.)?([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)*[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

	// guestFileEscaper covers the characters systemd unescapes inside a double-quoted EnvironmentFile value.
	guestFileEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "`", "\\`", `$`, `\$`)
)

// EnvInject sends a host-only entry's value to Hosts, exact or "*." suffix, as Header or as the Query parameter, where the guest sent a set Placeholder.
type EnvInject struct {
	Hosts       []string `json:"hosts"`
	Header      string   `json:"header,omitempty"`
	Query       string   `json:"query,omitempty"`
	Body        bool     `json:"body,omitzero"`
	Placeholder string   `json:"placeholder,omitempty"`
}

// Matches reports whether the lowercased host is one of the inject hosts.
func (i *EnvInject) Matches(host string) bool {
	return slices.ContainsFunc(i.Hosts, func(h string) bool {
		if suffix, wild := strings.CutPrefix(h, "*"); wild {
			return strings.HasSuffix(host, suffix)
		}
		return host == h
	})
}

func (i *EnvInject) validate(name string) error {
	if len(i.Hosts) == 0 || len(i.Hosts) > maxInjectHosts {
		return fmt.Errorf("env %s: inject needs 1 to %d hosts, got %d", name, maxInjectHosts, len(i.Hosts))
	}
	switch {
	case (i.Header == "") == (i.Query == ""):
		return fmt.Errorf("env %s: inject needs exactly one of header and query", name)
	case i.Header != "" && !httpguts.ValidHeaderFieldName(i.Header):
		return fmt.Errorf("env %s: inject header %q is not a valid header name", name, i.Header)
	case !httpguts.ValidHeaderFieldValue(i.Query):
		return fmt.Errorf("env %s: inject query must not hold control characters other than tab", name)
	case i.Query != "" && i.Placeholder == "":
		return fmt.Errorf("env %s: inject query needs a placeholder, so a value is never added to a request", name)
	case i.Body && i.Query == "":
		return fmt.Errorf("env %s: inject body needs query", name)
	}
	for _, h := range i.Hosts {
		if len(h) > 253 || !injectHostRe.MatchString(h) {
			return fmt.Errorf("env %s: inject host %q must be a lowercase host name or *.suffix", name, h)
		}
	}
	if len(i.Placeholder) > maxPlaceholder || !httpguts.ValidHeaderFieldValue(i.Placeholder) || strings.TrimSpace(i.Placeholder) != i.Placeholder {
		return fmt.Errorf("env %s: inject placeholder must be at most %d bytes of header value without control characters or surrounding space", name, maxPlaceholder)
	}
	return nil
}

func (i *EnvInject) size() int {
	n := len(i.Header) + len(i.Query) + len(i.Placeholder)
	for _, h := range i.Hosts {
		n += len(h)
	}
	return n
}

func (i *EnvInject) equal(other *EnvInject) bool {
	if i == nil || other == nil {
		return i == other
	}
	return i.Header == other.Header && i.Query == other.Query && i.Body == other.Body && i.Placeholder == other.Placeholder && slices.Equal(i.Hosts, other.Hosts)
}

// EnvVar is one entry of a claim's environment; a nil Guest means the entry reaches the guest.
type EnvVar struct {
	Value string `json:"value"`
	Guest *bool  `json:"guest,omitempty"`
	// Inject, on a host-only entry, makes the egress proxy send Value to the named hosts.
	Inject *EnvInject `json:"inject,omitempty"`
}

// InGuest reports whether the entry is delivered into the guest.
func (v EnvVar) InGuest() bool { return v.Guest == nil || *v.Guest }

// UnmarshalJSON rejects unknown members and a null guest, so a mistyped host-only flag never falls back to the guest.
func (v *EnvVar) UnmarshalJSON(b []byte) error {
	var raw struct {
		Value  string         `json:"value"`
		Guest  jsontext.Value `json:"guest,omitzero"`
		Inject *EnvInject     `json:"inject,omitempty"`
	}
	if err := json.Unmarshal(b, &raw, json.RejectUnknownMembers(true)); err != nil {
		return fmt.Errorf("env entry: %w", err)
	}
	*v = EnvVar{Value: raw.Value, Inject: raw.Inject}
	if raw.Guest == nil {
		return nil
	}
	var guest bool
	if raw.Guest.Kind() == 'n' || json.Unmarshal(raw.Guest, &guest) != nil {
		return errors.New("env entry: guest must be true or false")
	}
	v.Guest = &guest
	return nil
}

func (v EnvVar) same(other EnvVar) bool {
	return v.Value == other.Value && v.InGuest() == other.InGuest() && v.Inject.equal(other.Inject)
}

// Env is a claim's own environment: guest entries reach the guest, the others stay host-side for egress secrets.
type Env map[string]EnvVar

// Validate enforces the entry count, the name grammar, a bounded value a header and an env file both carry, and a bounded total.
func (e Env) Validate() error {
	if len(e) > maxEnvVars {
		return fmt.Errorf("env must contain at most %d entries, got %d", maxEnvVars, len(e))
	}
	total := 0
	for name, v := range e {
		total += len(name) + len(v.Value)
		if !EnvNameRe.MatchString(name) {
			return fmt.Errorf("env name %q must match %s", name, EnvNameRe)
		}
		if len(v.Value) > maxEnvValueBytes {
			return fmt.Errorf("env value of %s must be at most %d bytes, got %d", name, maxEnvValueBytes, len(v.Value))
		}
		if !httpguts.ValidHeaderFieldValue(v.Value) {
			return fmt.Errorf("env value of %s must not hold control characters other than tab", name)
		}
		if v.Inject == nil {
			continue
		}
		if v.InGuest() {
			return fmt.Errorf("env %s: inject needs guest: false, or the guest holds the value", name)
		}
		if err := v.Inject.validate(name); err != nil {
			return err
		}
		total += v.Inject.size()
	}
	if total > maxEnvBytes {
		return fmt.Errorf("env names, values and injects must total at most %d bytes, got %d", maxEnvBytes, total)
	}
	return nil
}

// HasInject reports whether any entry injects into egress requests.
func (e Env) HasInject() bool {
	for _, v := range e {
		if v.Inject != nil {
			return true
		}
	}
	return false
}

// InjectsChanged returns the entries of e that inject and that prev lacks or holds differently.
func (e Env) InjectsChanged(prev Env) Env {
	var out Env
	for name, v := range e {
		if old, ok := prev[name]; v.Inject != nil && (!ok || !v.same(old)) {
			if out == nil {
				out = Env{}
			}
			out[name] = v
		}
	}
	return out
}

// HasGuest reports whether any entry reaches the guest.
func (e Env) HasGuest() bool {
	for _, v := range e {
		if v.InGuest() {
			return true
		}
	}
	return false
}

// Equal reports whether e and other hold the same entries.
func (e Env) Equal(other Env) bool {
	return maps.EqualFunc(e, other, EnvVar.same)
}

// SameGuest reports whether e and other deliver the same guest entries.
func (e Env) SameGuest(other Env) bool {
	return maps.EqualFunc(e.guest(), other.guest(), EnvVar.same)
}

// Redacted copies e with every host-only value dropped, so a read names them without serving them.
func (e Env) Redacted() Env {
	out := maps.Clone(e)
	for name, v := range out {
		if !v.InGuest() {
			v.Value = ""
			out[name] = v
		}
	}
	return out
}

// GuestFile renders the guest entries as a systemd EnvironmentFile, sorted by name.
func (e Env) GuestFile() []byte {
	var b strings.Builder
	for _, name := range slices.Sorted(maps.Keys(e.guest())) {
		fmt.Fprintf(&b, "%s=\"%s\"\n", name, guestFileEscaper.Replace(e[name].Value))
	}
	return []byte(b.String())
}

func (e Env) guest() Env {
	out := Env{}
	for name, v := range e {
		if v.InGuest() {
			out[name] = v
		}
	}
	return out
}

// EnvPatch names the entries of a claim's env to set; a null entry removes the name.
type EnvPatch map[string]*EnvVar

// Validate enforces the name grammar on every entry and the Env rules on the entries it sets.
func (p EnvPatch) Validate() error {
	set := Env{}
	for name, v := range p {
		if !EnvNameRe.MatchString(name) {
			return fmt.Errorf("env name %q must match %s", name, EnvNameRe)
		}
		if v != nil {
			set[name] = *v
		}
	}
	return set.Validate()
}

// Apply returns e with p's entries set or removed, nil when none remain; e is left unchanged.
func (p EnvPatch) Apply(e Env) Env {
	out := Env{}
	maps.Copy(out, e)
	for name, v := range p {
		if v == nil {
			delete(out, name)
		} else {
			out[name] = *v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
