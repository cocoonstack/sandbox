// Package egress is a sandbox's only route out: a forward proxy that enforces its policy and injects node-side credentials.
package egress

import (
	"encoding/json/jsontext"
	"fmt"
	"net/http"
	"slices"
	"strings"
)

const (
	DecisionDeny Decision = iota
	DecisionAllow

	InterceptOff    InterceptMode = 0
	InterceptInject InterceptMode = 1
	InterceptAlways InterceptMode = 2
)

// Decision is the policy verdict for one request.
type Decision int

// InterceptMode is a rule's "intercept": false, true, or "inject" for only the claims that inject on the host; a higher mode outranks a lower one for the same host.
type InterceptMode uint8

func (m InterceptMode) MarshalJSONTo(enc *jsontext.Encoder) error {
	switch m {
	case InterceptAlways:
		return enc.WriteToken(jsontext.True)
	case InterceptInject:
		return enc.WriteToken(jsontext.String("inject"))
	default:
		return enc.WriteToken(jsontext.False)
	}
}

func (m *InterceptMode) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	tok, err := dec.ReadToken()
	if err != nil {
		return err
	}
	switch tok.Kind() {
	case 'f':
		*m = InterceptOff
	case 't':
		*m = InterceptAlways
	case '"':
		if tok.String() == "inject" {
			*m = InterceptInject
			return nil
		}
		fallthrough
	default:
		return fmt.Errorf("intercept must be true, false or \"inject\", got %s", tok)
	}
	return nil
}

// Rule allows requests matching Host (exact, "*." suffix, or "*"), Methods and Ports; empty means any.
type Rule struct {
	Host      string        `json:"host"`
	Methods   []string      `json:"methods,omitempty"`
	Ports     []uint16      `json:"ports,omitempty"`
	Secret    string        `json:"secret,omitempty"`
	Intercept InterceptMode `json:"intercept,omitzero"`
}

// Covers reports whether the rule matches every host the pattern, exact or "*." suffix, matches.
func (r Rule) Covers(pattern string) bool {
	suffix, wild := strings.CutPrefix(pattern, "*")
	if !wild {
		return r.matchHost(pattern)
	}
	rule, ruleWild := strings.CutPrefix(strings.ToLower(r.Host), "*")
	return ruleWild && strings.HasSuffix(suffix, rule)
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

// Intercepts reports whether any rule may terminate HTTPS; nil is false.
func (p *Policy) Intercepts() bool {
	return p != nil && slices.ContainsFunc(p.Allow, func(r Rule) bool { return r.Intercept != InterceptOff })
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
		if r.Intercept == InterceptInject && r.Secret != "" {
			return fmt.Errorf("allow[%d]: an intercept \"inject\" rule splices for claims without a credential, so it must not carry a secret", i)
		}
		for j, port := range r.Ports {
			if port == 0 {
				return fmt.Errorf("allow[%d]: port 0 is not a destination", i)
			}
			if slices.Contains(r.Ports[:j], port) {
				return fmt.Errorf("allow[%d]: port %d repeated", i, port)
			}
		}
	}
	if p.Socks5 && !p.admitsTunnel() {
		return fmt.Errorf("socks5 needs a rule that admits CONNECT")
	}
	return nil
}

// Eval returns the first matching rule that may carry a request unintercepted; an always-intercept rule here would leak its secret.
func (p Policy) Eval(host, method string, port uint16) (Rule, Decision) {
	host = strings.ToLower(host)
	for _, r := range p.Allow {
		if r.Intercept != InterceptAlways && r.matches(host, method, port) {
			return r, DecisionAllow
		}
	}
	return Rule{}, DecisionDeny
}

// EvalHost matches by host and port only; the highest intercept mode wins, the first rule on a tie.
func (p Policy) EvalHost(host string, port uint16) (Rule, Decision) {
	host = strings.ToLower(host)
	best := -1
	for i, r := range p.Allow {
		switch {
		case !r.matchHost(host) || !r.matchPort(port):
		case r.Intercept == InterceptAlways:
			return r, DecisionAllow
		case best < 0 || r.Intercept > p.Allow[best].Intercept:
			best = i
		}
	}
	if best < 0 {
		return Rule{}, DecisionDeny
	}
	return p.Allow[best], DecisionAllow
}

// EvalInner matches only rules of the mode EvalHost picks for the host, so a plain rule cannot shadow or rescue an intercept rule and an always rule shadows an inject rule.
func (p Policy) EvalInner(host, method string, port uint16) (Rule, Decision) {
	top, d := p.EvalHost(host, port)
	if d == DecisionDeny || top.Intercept == InterceptOff {
		return Rule{}, DecisionDeny
	}
	host = strings.ToLower(host)
	for _, r := range p.Allow {
		if r.Intercept == top.Intercept && r.matches(host, method, port) {
			return r, DecisionAllow
		}
	}
	return Rule{}, DecisionDeny
}

// ServesSocks reports whether the policy opted into the SOCKS5 door; Validate holds the rule that makes the door usable.
func (p Policy) ServesSocks() bool {
	return p.Socks5
}

// InterceptsHost reports whether an intercept rule covers the host pattern, so a claim credential for it can be injected.
func (p Policy) InterceptsHost(pattern string) bool {
	return slices.ContainsFunc(p.Allow, func(r Rule) bool { return r.Intercept != InterceptOff && r.Covers(pattern) })
}

func (p Policy) admitsTunnel() bool {
	return slices.ContainsFunc(p.Allow, func(r Rule) bool { return r.Intercept != InterceptAlways && r.matchMethod(http.MethodConnect) })
}

// Evaluator is what the proxy consults per request.
type Evaluator interface {
	Eval(host, method string, port uint16) (Rule, Decision)
	EvalHost(host string, port uint16) (Rule, Decision)
	EvalInner(host, method string, port uint16) (Rule, Decision)
	ServesSocks() bool
	InterceptsHost(pattern string) bool
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

func (c composite) InterceptsHost(pattern string) bool {
	return c.pool.InterceptsHost(pattern) && slices.ContainsFunc(c.tenant.Allow, func(r Rule) bool { return r.Covers(pattern) })
}
