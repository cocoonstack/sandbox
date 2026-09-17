// Package egress is the host-side guarded-egress data plane: a forward proxy
// a sandbox reaches as its only route out. Every request is evaluated against
// a per-sandbox policy (domain allow-list, methods) before it leaves the node,
// and a matched rule may inject a node-side credential the guest never holds.
package egress

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
)

const (
	DecisionDeny Decision = iota
	DecisionAllow
)

// Decision is the policy verdict for one request.
type Decision int

// Rule allows requests matching Host (exact, "*." suffix, or "*"), Methods and Ports; empty means any.
type Rule struct {
	Host      string   `json:"host"`
	Methods   []string `json:"methods,omitempty"`
	Ports     []uint16 `json:"ports,omitempty"`
	Secret    string   `json:"secret,omitempty"` //nolint:gosec // reference name of a node-side secret, never a value
	Intercept bool     `json:"intercept,omitzero"`
}

// matches expects host already lowercased by Eval.
func (r Rule) matches(host, method string, port uint16) bool {
	return r.matchHost(host) && r.matchMethod(method) && r.matchPort(port)
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

func (r Rule) matchPort(port uint16) bool {
	return len(r.Ports) == 0 || slices.Contains(r.Ports, port)
}

// Policy is one sandbox's egress allow-list; a request matching no rule is denied.
type Policy struct {
	Allow  []Rule `json:"allow"`
	Socks5 bool   `json:"socks5,omitzero"` // opts the SOCKS5 door in; a rule must still admit CONNECT
}

// Intercepts reports whether any rule terminates HTTPS; nil is false.
func (p *Policy) Intercepts() bool {
	return p != nil && slices.ContainsFunc(p.Allow, func(r Rule) bool { return r.Intercept })
}

// Validate rejects a policy the proxy could never honor; an empty allow-list is valid and denies everything.
func (p Policy) Validate() error {
	for i, r := range p.Allow {
		switch r.Host {
		case "":
			return fmt.Errorf("allow[%d]: host must not be empty", i)
		case "*.":
			return fmt.Errorf("allow[%d]: host %q needs a domain after the wildcard", i, r.Host)
		}
		seen := make(map[uint16]struct{}, len(r.Ports))
		for _, port := range r.Ports {
			if port == 0 {
				return fmt.Errorf("allow[%d]: port 0 is not a destination", i)
			}
			if _, dup := seen[port]; dup {
				return fmt.Errorf("allow[%d]: port %d repeated", i, port)
			}
			seen[port] = struct{}{}
		}
	}
	if p.Socks5 && !p.admitsTunnel() {
		return fmt.Errorf("socks5 needs a rule that admits CONNECT")
	}
	return nil
}

// Eval returns the first matching non-intercept rule; matching one here would leak its secret.
func (p Policy) Eval(host, method string, port uint16) (Rule, Decision) {
	host = strings.ToLower(host)
	for _, r := range p.Allow {
		if !r.Intercept && r.matches(host, method, port) {
			return r, DecisionAllow
		}
	}
	return Rule{}, DecisionDeny
}

// EvalHost matches by host and port only, preferring an intercept rule over an earlier plain match.
func (p Policy) EvalHost(host string, port uint16) (Rule, Decision) {
	host = strings.ToLower(host)
	first := -1
	for i, r := range p.Allow {
		switch {
		case !r.matchHost(host) || !r.matchPort(port):
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
func (p Policy) EvalInner(host, method string, port uint16) (Rule, Decision) {
	host = strings.ToLower(host)
	for _, r := range p.Allow {
		if r.Intercept && r.matches(host, method, port) {
			return r, DecisionAllow
		}
	}
	return Rule{}, DecisionDeny
}

// ServesSocks reports whether the policy opted into the SOCKS5 door; Validate holds the rule that makes the door usable.
func (p Policy) ServesSocks() bool {
	return p.Socks5
}

func (p Policy) admitsTunnel() bool {
	return slices.ContainsFunc(p.Allow, func(r Rule) bool { return !r.Intercept && r.matchMethod(http.MethodConnect) })
}

// Evaluator is what the proxy consults per request.
type Evaluator interface {
	Eval(host, method string, port uint16) (Rule, Decision)
	EvalHost(host string, port uint16) (Rule, Decision)
	EvalInner(host, method string, port uint16) (Rule, Decision)
	ServesSocks() bool
}

type composite struct {
	pool, tenant Policy
}

// Compose intersects a pool and a tenant policy; the pool rule wins on a double allow.
func Compose(pool, tenant Policy) Evaluator {
	return composite{pool: pool, tenant: tenant}
}

func (c composite) Eval(host, method string, port uint16) (Rule, Decision) {
	rule, pd := c.pool.Eval(host, method, port)
	if pd != DecisionAllow {
		return Rule{}, DecisionDeny
	}
	if _, td := c.tenant.Eval(host, method, port); td != DecisionAllow {
		return Rule{}, DecisionDeny
	}
	return rule, DecisionAllow
}

func (c composite) EvalHost(host string, port uint16) (Rule, Decision) {
	rule, pd := c.pool.EvalHost(host, port)
	if pd != DecisionAllow {
		return Rule{}, DecisionDeny
	}
	if _, td := c.tenant.EvalHost(host, port); td != DecisionAllow {
		return Rule{}, DecisionDeny
	}
	return rule, DecisionAllow
}

func (c composite) EvalInner(host, method string, port uint16) (Rule, Decision) {
	rule, pd := c.pool.EvalInner(host, method, port)
	if pd != DecisionAllow {
		return Rule{}, DecisionDeny
	}
	if _, td := c.tenant.Eval(host, method, port); td != DecisionAllow {
		return Rule{}, DecisionDeny
	}
	return rule, DecisionAllow
}

// ServesSocks is the pool's call: the tenant's rules already gate every tunnel through Eval.
func (c composite) ServesSocks() bool {
	return c.pool.ServesSocks()
}
