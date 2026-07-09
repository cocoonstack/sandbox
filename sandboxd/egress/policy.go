// Package egress is the host-side guarded-egress data plane: a forward proxy
// a sandbox reaches as its only route out. Every request is evaluated against
// a per-sandbox policy (domain allow-list, methods) before it leaves the node,
// and a matched rule may inject a node-side credential the guest never holds.
package egress

import (
	"fmt"
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
// matched request; empty injects nothing. Host matching is case-insensitive.
type Rule struct {
	Host    string   `json:"host"`
	Methods []string `json:"methods,omitempty"`
	Secret  string   `json:"secret,omitempty"` //nolint:gosec // reference name of a node-side secret, never a value
}

func (r Rule) matches(host, method string) bool {
	return r.matchHost(host) && r.matchMethod(method)
}

func (r Rule) matchHost(host string) bool {
	host = strings.ToLower(host)
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
	if len(r.Methods) == 0 {
		return true
	}
	for _, m := range r.Methods {
		if strings.EqualFold(m, method) {
			return true
		}
	}
	return false
}

// Policy is one sandbox's egress allow-list. Default-deny is implicit: a
// request matching no Allow rule is denied.
type Policy struct {
	Allow []Rule `json:"allow"`
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

// Eval returns the first matching allow rule with DecisionAllow, or a zero
// rule with DecisionDeny when nothing matches. host is a bare hostname (no
// port); the proxy strips the port before calling.
func (p Policy) Eval(host, method string) (Rule, Decision) {
	for _, r := range p.Allow {
		if r.matches(host, method) {
			return r, DecisionAllow
		}
	}
	return Rule{}, DecisionDeny
}
