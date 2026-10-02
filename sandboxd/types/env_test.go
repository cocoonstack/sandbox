package types

import (
	"encoding/json/v2"
	"strings"
	"testing"
)

func TestEnvValidate(t *testing.T) {
	many := Env{}
	for i := range maxEnvVars + 1 {
		many["V"+strings.Repeat("X", i)] = EnvVar{}
	}
	full, over := Env{}, Env{}
	for i := range 8 {
		name := "V" + strings.Repeat("X", i)
		full[name] = EnvVar{Value: strings.Repeat("a", maxEnvBytes/8-len(name))}
		over[name] = EnvVar{Value: strings.Repeat("B=b", maxEnvValueBytes/3)}
	}
	tests := []struct {
		name string
		env  Env
		want string
	}{
		{"empty", nil, ""},
		{"guest and host-only", Env{"TOKEN": {Value: "Bearer a", Guest: new(false)}, "_x9": {Value: ""}}, ""},
		{"too many", many, "at most"},
		{"leading digit", Env{"9A": {}}, "env name"},
		{"dash", Env{"A-B": {}}, "env name"},
		{"newline", Env{"A": {Value: "a\nB=b"}}, "control characters"},
		{"nul", Env{"A": {Value: "a\x00"}}, "control characters"},
		{"at the bound", Env{"A": {Value: strings.Repeat("a", maxEnvValueBytes)}}, ""},
		{"over the bound", Env{"A": {Value: strings.Repeat("a", maxEnvValueBytes+1)}}, "at most"},
		{"total at the bound", full, ""},
		{"total over the bound", over, "total at most"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.env.Validate()
			if (tt.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tt.want)) {
				t.Errorf("Validate() = %v, want %q", err, tt.want)
			}
			if err != nil && strings.Contains(err.Error(), "B=b") {
				t.Errorf("error %q leaks the value", err)
			}
		})
	}
}

func TestEnvPatchAppliesAndValidates(t *testing.T) {
	base := Env{"A": {Value: "a"}, "H": {Value: "h", Guest: new(false)}}
	got := EnvPatch{"A": nil, "B": {Value: "b"}, "Z": nil}.Apply(base)
	if !got.Equal(Env{"B": {Value: "b"}, "H": {Value: "h", Guest: new(false)}}) || !base.Equal(Env{"A": {Value: "a"}, "H": {Value: "h", Guest: new(false)}}) {
		t.Errorf("Apply = %v (base %v)", got, base)
	}
	if got := (EnvPatch{"A": nil, "H": nil}).Apply(base); got != nil {
		t.Errorf("removing every entry = %v, want nil", got)
	}
	for _, tt := range []struct {
		patch EnvPatch
		want  string
	}{
		{EnvPatch{"A": nil, "B": {Value: "b"}}, ""},
		{EnvPatch{"A-B": nil}, "env name"},
		{EnvPatch{"A": {Value: "a\nB=b"}}, "control characters"},
	} {
		if err := tt.patch.Validate(); (tt.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tt.want)) {
			t.Errorf("Validate(%v) = %v, want %q", tt.patch, err, tt.want)
		}
	}
}

func TestEnvVarDecodesStrictly(t *testing.T) {
	for _, tt := range []struct {
		body  string
		guest bool
		ok    bool
	}{
		{`{"value":"s"}`, true, true},
		{`{"value":"s","guest":false}`, false, true},
		{`{"value":"s","guest":true}`, true, true},
		{`{"value":"s","guset":false}`, false, false},
		{`{"value":"s","host_only":true}`, false, false},
		{`{"value":"s","guest":null}`, false, false},
		{`{"value":"s","guest":"false"}`, false, false},
		{`{"value":"s","guest":false,"inject":{"hosts":["a.test"],"header":"X-Key"}}`, false, true},
		{`{"value":"s","guest":false,"inject":{"hosts":["a.test"],"heder":"X-Key"}}`, false, false},
	} {
		var v EnvVar
		err := json.Unmarshal([]byte(tt.body), &v)
		if (err == nil) != tt.ok || (err == nil && v.InGuest() != tt.guest) {
			t.Errorf("%s: err=%v guest=%t, want ok=%t guest=%t", tt.body, err, v.InGuest(), tt.ok, tt.guest)
		}
	}
}

func TestEnvInjectValidates(t *testing.T) {
	inject := func(header string, hosts ...string) Env {
		return Env{"K": {Value: "v", Guest: new(false), Inject: &EnvInject{Hosts: hosts, Header: header}}}
	}
	tests := []struct {
		name string
		env  Env
		want string
	}{
		{"host-only exact and wildcard", inject("X-Api-Key", "api.example.com", "*.corp.test", "10.0.0.5"), ""},
		{"guest entry", Env{"K": {Value: "v", Inject: &EnvInject{Hosts: []string{"a.test"}, Header: "X-Key"}}}, "needs guest: false"},
		{"no hosts", inject("X-Key"), "1 to 8 hosts"},
		{"too many hosts", inject("X-Key", "a", "b", "c", "d", "e", "f", "g", "h", "i"), "1 to 8 hosts"},
		{"uppercase host", inject("X-Key", "API.example.com"), "lowercase host name"},
		{"bare wildcard", inject("X-Key", "*"), "lowercase host name"},
		{"host with a port", inject("X-Key", "a.test:443"), "lowercase host name"},
		{"bad header", inject("X Key", "a.test"), "not a valid header name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.env.Validate()
			if (tt.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tt.want)) {
				t.Errorf("Validate = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestEnvInjectMatchesAndCompares(t *testing.T) {
	inj := &EnvInject{Hosts: []string{"api.example.com", "*.corp.test"}, Header: "X-Key"}
	for host, want := range map[string]bool{"api.example.com": true, "git.corp.test": true, "corp.test": false, "example.com": false} {
		if got := inj.Matches(host); got != want {
			t.Errorf("Matches(%q) = %v, want %v", host, got, want)
		}
	}
	a := Env{"K": {Value: "v", Guest: new(false), Inject: inj}}
	b := Env{"K": {Value: "v", Guest: new(false), Inject: &EnvInject{Hosts: []string{"api.example.com"}, Header: "X-Key"}}}
	if a.Equal(b) || !a.Equal(Env{"K": {Value: "v", Guest: new(false), Inject: inj}}) || !a.SameGuest(b) {
		t.Error("Equal must see an inject change, and SameGuest must not")
	}
	if red := a.Redacted(); red["K"].Value != "" || red["K"].Inject != inj {
		t.Errorf("Redacted = %+v, want the value dropped and the inject kept", red["K"])
	}
}

func TestSandboxPublishesItsEnvToTheProxy(t *testing.T) {
	sb := &Sandbox{}
	if _, ok := sb.HiddenEnv("K"); ok {
		t.Fatal("a sandbox without SetEnv served a hidden value")
	}
	to := func(hosts ...string) *EnvInject { return &EnvInject{Hosts: hosts, Header: "X-Key"} }
	sb.SetEnv(Env{
		"B":     {Value: "b", Guest: new(false), Inject: to("a.test")},
		"A":     {Value: "a", Guest: new(false), Inject: to("*.test")},
		"OTHER": {Value: "o", Guest: new(false), Inject: to("b.example")},
		"PLAIN": {Value: "p", Guest: new(false)},
	})
	var names []string
	for name := range sb.Injections("a.test") {
		names = append(names, name)
	}
	if strings.Join(names, ",") != "A,B" {
		t.Errorf("Injections(a.test) = %v, want A,B in name order", names)
	}
	if v, ok := sb.HiddenEnv("PLAIN"); !ok || v != "p" {
		t.Errorf("HiddenEnv(PLAIN) = %q %v, want p", v, ok)
	}
	sb.SetEnv(nil)
	if _, ok := sb.HiddenEnv("PLAIN"); ok {
		t.Error("a cleared env still serves a hidden value")
	}
}

func TestEnvSplitsGuestAndHostOnlyEntries(t *testing.T) {
	env := Env{"G": {Value: "g"}, "H": {Value: "h", Guest: new(false)}, "T": {Value: "t", Guest: new(true)}}
	if v, ok := env.Hidden("H"); !ok || v != "h" {
		t.Errorf("Hidden(H) = %q %v, want h", v, ok)
	}
	if _, ok := env.Hidden("G"); ok {
		t.Error("a guest entry resolved as host-only")
	}
	if !env.HasGuest() || (Env{"H": env["H"]}).HasGuest() {
		t.Error("HasGuest misreads the guest flag")
	}
	red := env.Redacted()
	if red["H"].Value != "" || red["G"].Value != "g" || env["H"].Value != "h" {
		t.Errorf("Redacted = %v (source %v), want only the host-only value dropped, source intact", red, env)
	}
	if !env.SameGuest(Env{"G": {Value: "g"}, "T": {Value: "t"}}) || env.SameGuest(Env{"G": {Value: "g"}}) {
		t.Error("SameGuest must compare only the guest entries")
	}
	if got := string(env.GuestFile()); got != "G=\"g\"\nT=\"t\"\n" {
		t.Errorf("GuestFile = %q", got)
	}
}
