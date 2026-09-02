package egress

import "testing"

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
			rule, got := policy.Eval(tt.host, tt.method)
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
		{Host: "api.github.com", Secret: "gh", Intercept: true},
		{Host: "plain.github.com"},
	}}

	if rule, d := p.Eval("api.github.com", "GET"); d != DecisionDeny || rule.Secret != "" {
		t.Errorf("Eval on an intercept-only host = %+v/%v, want deny", rule, d)
	}
	if _, d := p.Eval("plain.github.com", "GET"); d != DecisionAllow {
		t.Error("plain rule must still allow the forward path")
	}
	if _, d := p.EvalHost("api.github.com"); d != DecisionAllow {
		t.Error("EvalHost must still allow the intercepted CONNECT path")
	}
}

func TestEvalHostPrefersInterceptRule(t *testing.T) {
	p := Policy{Allow: []Rule{
		{Host: "api.github.com", Methods: []string{"GET"}},
		{Host: "api.github.com", Intercept: true},
	}}
	rule, d := p.EvalHost("api.github.com")
	if d != DecisionAllow || !rule.Intercept {
		t.Errorf("EvalHost = %+v/%v, want the intercept rule over the earlier plain match", rule, d)
	}
	if rule, d := p.EvalHost("other.com"); d != DecisionDeny || rule.Intercept {
		t.Errorf("EvalHost(other.com) = %+v/%v, want deny", rule, d)
	}
}

func TestEvalInnerMatchesOnlyInterceptRulesByMethod(t *testing.T) {
	p := Policy{Allow: []Rule{
		{Host: "*.example.com"},
		{Host: "api.example.com", Methods: []string{"GET"}, Secret: "gh", Intercept: true},
		{Host: "api.example.com", Methods: []string{"POST"}, Intercept: true},
	}}
	if rule, d := p.EvalInner("api.example.com", "GET"); d != DecisionAllow || rule.Secret != "gh" {
		t.Errorf("EvalInner GET = %+v/%v, want the GET intercept rule with secret gh", rule, d)
	}
	if rule, d := p.EvalInner("api.example.com", "POST"); d != DecisionAllow || rule.Secret != "" {
		t.Errorf("EvalInner POST = %+v/%v, want the POST intercept rule (later rule reachable)", rule, d)
	}
	if _, d := p.EvalInner("api.example.com", "DELETE"); d != DecisionDeny {
		t.Error("EvalInner DELETE allowed; the plain rule must not rescue a method no intercept rule covers")
	}
}

func TestCompositeEvalInnerIntersectsTenant(t *testing.T) {
	pool := Policy{Allow: []Rule{{Host: "api.example.com", Secret: "gh", Intercept: true}}}
	ev := Compose(pool, Policy{Allow: []Rule{{Host: "api.example.com", Methods: []string{"GET"}}}})
	if rule, d := ev.EvalInner("api.example.com", "GET"); d != DecisionAllow || rule.Secret != "gh" {
		t.Errorf("EvalInner GET = %+v/%v, want allow with the pool secret", rule, d)
	}
	if _, d := ev.EvalInner("api.example.com", "POST"); d != DecisionDeny {
		t.Error("EvalInner POST allowed though the tenant permits only GET; want tenant intersection")
	}
}

func TestWildcardApexIsNotMatched(t *testing.T) {
	policy := Policy{Allow: []Rule{{Host: "*.example.com"}}}
	if _, d := policy.Eval("example.com", "GET"); d != DecisionDeny {
		t.Errorf("apex example.com matched *.example.com, want deny")
	}
	if _, d := policy.Eval("a.example.com", "GET"); d != DecisionAllow {
		t.Errorf("a.example.com did not match *.example.com, want allow")
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.policy.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
