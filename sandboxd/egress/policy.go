// Package egress is the host-side guarded-egress data plane: a forward proxy
// a sandbox reaches as its only route out. Every request is evaluated against
// a per-sandbox policy (domain allow-list, methods) before it leaves the node,
// and a matched rule may inject a node-side credential the guest never holds.
package egress

import (
	"fmt"
	"slices"
	"strings"
)

const (
	DecisionDeny Decision = iota
	DecisionAllow
)

// Decision is the policy verdict for one request.
type Decision int

// Rule allows requests whose host matches Host — an exact name, a
// "*."-prefixed suffix wildcard ("*.example.com" matches sub.example.com but
// not the apex), or "*" for any host — and whose method is in Methods (empty
// means any). Secret names a node-side credential the proxy injects into a
// matched request; empty injects nothing. Intercept terminates a matched
// HTTPS CONNECT so the request is filtered by method and the secret injected;
// only a pool rule may set it. Host matching is case-insensitive.
type Rule struct {
	Host      string   `json:"host"`
	Methods   []string `json:"methods,omitempty"`
	Secret    string   `json:"secret,omitempty"` //nolint:gosec // reference name of a node-side secret, never a value
	Intercept bool     `json:"intercept,omitempty"`
}

// matches expects host already lowercased by Eval.
func (r Rule) matches(host, method string) bool {
	return r.matchHost(host) && r.matchMethod(method)
}

func (r Rule) matchHost(host string) bool {
	switch {
	case r.Host == "*":
		return true
	case strings.HasPrefix(r.Host, "*."):
		return strings.HasSuffix(host, strings.ToLower(r.Host[1:]))
	default:
		return host == strings.ToLower(r.Host)
	}
}

func (r Rule) matchMethod(method string) bool {
	return len(r.Methods) == 0 ||
		slices.ContainsFunc(r.Methods, func(m string) bool { return strings.EqualFold(m, method) })
}

// Policy is one sandbox's egress allow-list. Default-deny is implicit: a
// request matching no Allow rule is denied.
type Policy struct {
	Allow []Rule `json:"allow"`
}

// Intercepts reports whether any rule terminates HTTPS; nil is false. It gates
// baking the cluster root cert into a pool's golden.
func (p *Policy) Intercepts() bool {
	return p != nil && slices.ContainsFunc(p.Allow, func(r Rule) bool { return r.Intercept })
}

// Validate rejects a rule with an empty or bare-wildcard host; an empty
// allow-list is valid (it denies everything).
func (p Policy) Validate() error {
	for i, r := range p.Allow {
		switch r.Host {
		case "":
			return fmt.Errorf("allow[%d]: host must not be empty", i)
		case "*.":
			return fmt.Errorf("allow[%d]: host %q needs a domain after the wildcard", i, r.Host)
		}
	}
	return nil
}

// Eval returns the first matching non-intercept allow rule with DecisionAllow,
// or a zero rule with DecisionDeny. host is a bare hostname (no port). Intercept
// rules answer only the CONNECT paths (EvalHost/EvalInner) — matching one here
// would inject its secret over cleartext.
func (p Policy) Eval(host, method string) (Rule, Decision) {
	host = strings.ToLower(host)
	for _, r := range p.Allow {
		if !r.Intercept && r.matches(host, method) {
			return r, DecisionAllow
		}
	}
	return Rule{}, DecisionDeny
}

// EvalHost matches by host only (a CONNECT tunnel carries no method), preferring
// an intercept rule over an earlier plain match: interception is a host property.
func (p Policy) EvalHost(host string) (Rule, Decision) {
	host = strings.ToLower(host)
	first := -1
	for i, r := range p.Allow {
		switch {
		case !r.matchHost(host):
		case r.Intercept:
			return r, DecisionAllow
		case first < 0:
			first = i
		}
	}
	if first >= 0 {
		return p.Allow[first], DecisionAllow
	}
	return Rule{}, DecisionDeny
}

// EvalInner decides a decrypted request over an intercepted tunnel: it matches
// only intercept rules, by host+method, so a plain rule can neither shadow the
// injection nor rescue a method the intercept rules reject.
func (p Policy) EvalInner(host, method string) (Rule, Decision) {
	host = strings.ToLower(host)
	for _, r := range p.Allow {
		if r.Intercept && r.matches(host, method) {
			return r, DecisionAllow
		}
	}
	return Rule{}, DecisionDeny
}

// Evaluator is what the proxy consults per request; Policy is one, Compose
// intersects two.
type Evaluator interface {
	Eval(host, method string) (Rule, Decision)
	EvalHost(host string) (Rule, Decision)
	EvalInner(host, method string) (Rule, Decision)
}

// Compose intersects a pool and a tenant policy: a request must pass both, and
// the pool rule (with its secret) wins on a double allow.
func Compose(pool, tenant Policy) Evaluator {
	return composite{pool: pool, tenant: tenant}
}

type composite struct {
	pool, tenant Policy
}

func (c composite) Eval(host, method string) (Rule, Decision) {
	rule, pd := c.pool.Eval(host, method)
	if pd != DecisionAllow {
		return Rule{}, DecisionDeny
	}
	if _, td := c.tenant.Eval(host, method); td != DecisionAllow {
		return Rule{}, DecisionDeny
	}
	return rule, DecisionAllow
}

func (c composite) EvalHost(host string) (Rule, Decision) {
	rule, pd := c.pool.EvalHost(host)
	if pd != DecisionAllow {
		return Rule{}, DecisionDeny
	}
	if _, td := c.tenant.EvalHost(host); td != DecisionAllow {
		return Rule{}, DecisionDeny
	}
	return rule, DecisionAllow
}

func (c composite) EvalInner(host, method string) (Rule, Decision) {
	rule, pd := c.pool.EvalInner(host, method)
	if pd != DecisionAllow {
		return Rule{}, DecisionDeny
	}
	if _, td := c.tenant.Eval(host, method); td != DecisionAllow {
		return Rule{}, DecisionDeny
	}
	return rule, DecisionAllow
}
