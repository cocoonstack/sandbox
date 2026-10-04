package egress

import (
	"encoding/json/v2"
	"testing"
)

func TestPolicyEval(t *testing.T) {
	policy := Policy{Allow: []Rule{
		{Host: "api.github.com", Methods: []string{"GET", "POST"}, Secret: "gh"},
		{Host: "*.googleapis.com"},
		{Host: "*", Methods: []string{"GET"}},
	}}
	tests := []struct {
		name   string
		host   string
		method string
		want   Decision
		secret string
	}{
		{"exact host and method", "api.github.com", "GET", DecisionAllow, "gh"},
		{"exact host case-insensitive", "API.GitHub.com", "POST", DecisionAllow, "gh"},
		{"exact host wrong method falls through to catch-all deny", "api.github.com", "DELETE", DecisionDeny, ""},
		{"wildcard subdomain", "storage.googleapis.com", "PUT", DecisionAllow, ""},
		{"wildcard does not match apex", "googleapis.com", "GET", DecisionAllow, ""},
		{"catch-all get", "example.com", "GET", DecisionAllow, ""},
		{"catch-all denies non-get", "example.com", "POST", DecisionDeny, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule, got := policy.Eval(tt.host, tt.method, 443)
			if got != tt.want {
				t.Fatalf("Eval(%q,%q) = %v, want %v", tt.host, tt.method, got, tt.want)
			}
			if rule.Secret != tt.secret {
				t.Errorf("Eval(%q,%q) secret = %q, want %q", tt.host, tt.method, rule.Secret, tt.secret)
			}
		})
	}
}

func TestEvalSkipsInterceptRules(t *testing.T) {
	p := Policy{Allow: []Rule{
		{Host: "api.github.com", Secret: "gh", Intercept: InterceptAlways},
		{Host: "plain.github.com"},
	}}

	if rule, d := p.Eval("api.github.com", "GET", 443); d != DecisionDeny || rule.Secret != "" {
		t.Errorf("Eval on an intercept-only host = %+v/%v, want deny", rule, d)
	}
	if _, d := p.Eval("plain.github.com", "GET", 443); d != DecisionAllow {
		t.Error("plain rule must still allow the forward path")
	}
	if _, d := p.EvalHost("api.github.com", 443); d != DecisionAllow {
		t.Error("EvalHost must still allow the intercepted CONNECT path")
	}
	inject := Policy{Allow: []Rule{{Host: "*", Intercept: InterceptInject}}}
	if _, d := inject.Eval("any.test", "CONNECT", 443); d != DecisionAllow {
		t.Error("Eval skipped an inject rule, which splices or forwards for a claim without a credential")
	}
}

func TestInterceptModeRoundTripsFalseTrueAndInjectAndRefusesOthers(t *testing.T) {
	var p Policy
	if err := json.Unmarshal([]byte(`{"allow":[{"host":"a"},{"host":"b","intercept":false},{"host":"c","intercept":true},{"host":"d","intercept":"inject"}]}`), &p); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for i, mode := range []InterceptMode{InterceptOff, InterceptOff, InterceptAlways, InterceptInject} {
		if p.Allow[i].Intercept != mode {
			t.Errorf("rule %s intercept = %d, want %d", p.Allow[i].Host, p.Allow[i].Intercept, mode)
		}
	}
	out, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"allow":[{"host":"a"},{"host":"b"},{"host":"c","intercept":true},{"host":"d","intercept":"inject"}]}`
	if got := string(out); got != want {
		t.Errorf("Marshal = %s, want %s", got, want)
	}
	for _, bad := range []string{`"always"`, `1`, `null`} {
		if err := json.Unmarshal([]byte(`{"allow":[{"host":"a","intercept":`+bad+`}]}`), &p); err == nil {
			t.Errorf("intercept %s decoded", bad)
		}
	}
}

func TestEvalHostPrefersInterceptRule(t *testing.T) {
	p := Policy{Allow: []Rule{
		{Host: "api.github.com", Methods: []string{"GET"}},
		{Host: "api.github.com", Intercept: InterceptAlways},
	}}
	rule, d := p.EvalHost("api.github.com", 443)
	if d != DecisionAllow || rule.Intercept != InterceptAlways {
		t.Errorf("EvalHost = %+v/%v, want the intercept rule over the earlier plain match", rule, d)
	}
	if rule, d := p.EvalHost("other.com", 443); d != DecisionDeny || rule.Intercept != InterceptOff {
		t.Errorf("EvalHost(other.com) = %+v/%v, want deny", rule, d)
	}
}

func TestEvalInnerMatchesOnlyInterceptRulesByMethod(t *testing.T) {
	p := Policy{Allow: []Rule{
		{Host: "*.example.com"},
		{Host: "api.example.com", Methods: []string{"GET"}, Secret: "gh", Intercept: InterceptAlways},
		{Host: "api.example.com", Methods: []string{"POST"}, Intercept: InterceptAlways},
	}}
	if rule, d := p.EvalInner("api.example.com", "GET", 443); d != DecisionAllow || rule.Secret != "gh" {
		t.Errorf("EvalInner GET = %+v/%v, want the GET intercept rule with secret gh", rule, d)
	}
	if rule, d := p.EvalInner("api.example.com", "POST", 443); d != DecisionAllow || rule.Secret != "" {
		t.Errorf("EvalInner POST = %+v/%v, want the POST intercept rule (later rule reachable)", rule, d)
	}
	if _, d := p.EvalInner("api.example.com", "DELETE", 443); d != DecisionDeny {
		t.Error("EvalInner DELETE allowed; the plain rule must not rescue a method no intercept rule covers")
	}
}

func TestCompositeEvalInnerIntersectsTenant(t *testing.T) {
	pool := Policy{Allow: []Rule{{Host: "api.example.com", Secret: "gh", Intercept: InterceptAlways}}}
	ev := Compose(pool, Policy{Allow: []Rule{{Host: "api.example.com", Methods: []string{"GET"}}}})
	if rule, d := ev.EvalInner("api.example.com", "GET", 443); d != DecisionAllow || rule.Secret != "gh" {
		t.Errorf("EvalInner GET = %+v/%v, want allow with the pool secret", rule, d)
	}
	if _, d := ev.EvalInner("api.example.com", "POST", 443); d != DecisionDeny {
		t.Error("EvalInner POST allowed though the tenant permits only GET; want tenant intersection")
	}
}

func TestInterceptsHostCoversPatterns(t *testing.T) {
	pool := Policy{Allow: []Rule{
		{Host: "api.example.com", Intercept: InterceptAlways},
		{Host: "*.Corp.Test", Intercept: InterceptAlways},
		{Host: "plain.example.com"},
	}}
	for pattern, want := range map[string]bool{
		"api.example.com":   true,
		"*.example.com":     false,
		"git.corp.test":     true,
		"*.corp.test":       true,
		"*.eu.corp.test":    true,
		"corp.test":         false,
		"plain.example.com": false,
	} {
		if got := pool.InterceptsHost(pattern); got != want {
			t.Errorf("pool InterceptsHost(%q) = %v, want %v", pattern, got, want)
		}
	}
	if !(Policy{Allow: []Rule{{Host: "*", Intercept: InterceptAlways}}}).InterceptsHost("*.anything.test") {
		t.Error("a bare * intercept rule does not cover a wildcard pattern")
	}
	composed := Compose(pool, Policy{Allow: []Rule{{Host: "*.corp.test"}}})
	if composed.InterceptsHost("api.example.com") || !composed.InterceptsHost("git.corp.test") {
		t.Error("a composed policy must need the pool to intercept and the tenant to allow the pattern")
	}
}

func TestWildcardApexIsNotMatched(t *testing.T) {
	policy := Policy{Allow: []Rule{{Host: "*.example.com"}}}
	if _, d := policy.Eval("example.com", "GET", 443); d != DecisionDeny {
		t.Error("apex example.com matched *.example.com, want deny")
	}
	if _, d := policy.Eval("a.example.com", "GET", 443); d != DecisionAllow {
		t.Error("a.example.com did not match *.example.com, want allow")
	}
}

func TestPolicyValidate(t *testing.T) {
	tests := []struct {
		name    string
		policy  Policy
		wantErr bool
	}{
		{"empty allow-list is valid", Policy{}, false},
		{"good rules", Policy{Allow: []Rule{{Host: "api.github.com"}, {Host: "*.dev"}, {Host: "*"}}}, false},
		{"empty host", Policy{Allow: []Rule{{Host: ""}}}, true},
		{"bare wildcard", Policy{Allow: []Rule{{Host: "*."}}}, true},
		{"socks5 with a tunnel rule", Policy{Socks5: true, Allow: []Rule{{Host: "api.github.com"}}}, false},
		{"socks5 without a tunnel rule", Policy{Socks5: true, Allow: []Rule{{Host: "api.github.com", Methods: []string{"GET"}}}}, true},
		{"ports", Policy{Allow: []Rule{{Host: "api.github.com", Ports: []uint16{443, 993}}}}, false},
		{"port 0", Policy{Allow: []Rule{{Host: "api.github.com", Ports: []uint16{0}}}}, true},
		{"repeated port", Policy{Allow: []Rule{{Host: "api.github.com", Ports: []uint16{443, 443}}}}, true},
		{"inject rule with a secret", Policy{Allow: []Rule{{Host: "*", Secret: "gh", Intercept: InterceptInject}}}, true},
		{"socks5 with only an inject rule", Policy{Socks5: true, Allow: []Rule{{Host: "*", Intercept: InterceptInject}}}, false},
		{"socks5 with only an intercept rule", Policy{Socks5: true, Allow: []Rule{{Host: "*", Intercept: InterceptAlways}}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.policy.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPolicyServesSocks(t *testing.T) {
	opted := Policy{Socks5: true, Allow: []Rule{{Host: "a.internal"}}}
	silent := Policy{Allow: []Rule{{Host: "a.internal"}}}
	if !opted.ServesSocks() || silent.ServesSocks() {
		t.Errorf("ServesSocks() opted=%v silent=%v, want true/false", opted.ServesSocks(), silent.ServesSocks())
	}
	if !Compose(opted, silent).ServesSocks() {
		t.Error("composite ServesSocks() = false with an opted-in pool and a silent tenant; the pool owns the door")
	}
	if Compose(silent, opted).ServesSocks() {
		t.Error("composite ServesSocks() = true with a silent pool; a tenant cannot open the door alone")
	}
}

func TestRulePorts(t *testing.T) {
	policy := Policy{Allow: []Rule{
		{Host: "db.internal", Ports: []uint16{5432}},
		{Host: "mail.internal", Ports: []uint16{993, 465}, Intercept: InterceptAlways},
		{Host: "*.open.internal"},
	}}
	tests := []struct {
		name string
		host string
		port uint16
		want Decision
	}{
		{"listed port", "db.internal", 5432, DecisionAllow},
		{"unlisted port", "db.internal", 5433, DecisionDeny},
		{"no ports means any", "a.open.internal", 8080, DecisionAllow},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, got := policy.Eval(tt.host, "CONNECT", tt.port); got != tt.want {
				t.Errorf("Eval(%q, CONNECT, %d) = %v, want %v", tt.host, tt.port, got, tt.want)
			}
		})
	}
	if rule, d := policy.EvalHost("mail.internal", 993); d != DecisionAllow || rule.Intercept != InterceptAlways {
		t.Errorf("EvalHost(mail.internal, 993) = %+v/%v, want the intercept rule", rule, d)
	}
	if _, d := policy.EvalHost("mail.internal", 143); d != DecisionDeny {
		t.Error("EvalHost(mail.internal, 143) allowed; the intercept rule lists 993 and 465 only")
	}
	if _, d := policy.EvalInner("mail.internal", "GET", 143); d != DecisionDeny {
		t.Error("EvalInner(mail.internal, GET, 143) allowed; want the port checked on the inner request too")
	}
	ev := Compose(
		Policy{Allow: []Rule{{Host: "db.internal", Ports: []uint16{5432, 6432}}}},
		Policy{Allow: []Rule{{Host: "db.internal", Ports: []uint16{6432}}}},
	)
	if _, d := ev.Eval("db.internal", "CONNECT", 5432); d != DecisionDeny {
		t.Error("composite allowed 5432 though the tenant lists 6432 only; want intersection")
	}
	if _, d := ev.Eval("db.internal", "CONNECT", 6432); d != DecisionAllow {
		t.Error("composite denied 6432 though both sides list it")
	}
}
