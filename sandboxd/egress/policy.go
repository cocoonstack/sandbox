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

// Rule allows requests matching Host (exact, "*." suffix, or "*") and Methods; empty means any.
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

// Policy is one sandbox's egress allow-list; a request matching no rule is denied.
type Policy struct {
	Allow []Rule `json:"allow"`
}

// Intercepts reports whether any rule terminates HTTPS; nil is false.
func (p *Policy) Intercepts() bool {
	return p != nil && slices.ContainsFunc(p.Allow, func(r Rule) bool { return r.Intercept })
}

// Validate rejects an empty or bare-wildcard host; an empty allow-list denies everything.
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

// Eval returns the first matching non-intercept rule; matching one here would leak its secret.
func (p Policy) Eval(host, method string) (Rule, Decision) {
	host = strings.ToLower(host)
	for _, r := range p.Allow {
		if !r.Intercept && r.matches(host, method) {
			return r, DecisionAllow
		}
	}
	return Rule{}, DecisionDeny
}

// EvalHost matches by host only, preferring an intercept rule over an earlier plain match.
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

// EvalInner matches only intercept rules, so a plain rule cannot shadow or rescue one.
func (p Policy) EvalInner(host, method string) (Rule, Decision) {
	host = strings.ToLower(host)
	for _, r := range p.Allow {
		if r.Intercept && r.matches(host, method) {
			return r, DecisionAllow
		}
	}
	return Rule{}, DecisionDeny
}

// Evaluator is what the proxy consults per request.
type Evaluator interface {
	Eval(host, method string) (Rule, Decision)
	EvalHost(host string) (Rule, Decision)
	EvalInner(host, method string) (Rule, Decision)
}

// Compose intersects a pool and a tenant policy; the pool rule wins on a double allow.
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
