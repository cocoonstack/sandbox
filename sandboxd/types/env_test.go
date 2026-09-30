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
	} {
		var v EnvVar
		err := json.Unmarshal([]byte(tt.body), &v)
		if (err == nil) != tt.ok || (err == nil && v.InGuest() != tt.guest) {
			t.Errorf("%s: err=%v guest=%t, want ok=%t guest=%t", tt.body, err, v.InGuest(), tt.ok, tt.guest)
		}
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
