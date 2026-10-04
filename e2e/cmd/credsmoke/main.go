// credsmoke is the claim-credential acceptance: claims each send their own env-injected header,
// query or body credential through the proxy (-inject-size: on an "inject" pool), the guest never
// holds the value, and rotation, refusal, clearing and forks behave as the API says.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/e2e/internal/harness"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

const (
	proxy  = "http://127.0.0.1:3128"
	header = "X-Api-Key"
	entry  = "API_KEY"
	tsPH   = "@TS@"
	serpPH = "@SERP@"
	fbPH   = "@FB@"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7882", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "template of the intercept pool")
	echo := flag.String("echo", "postman-echo.com", "HTTPS host that echoes request headers at /headers")
	secret := flag.String("secret", "", "the pool secret's value the origin should also observe")
	reattach := flag.String("reattach", "", "id:token:value of a claim to recheck after a restart")
	sse := flag.String("sse", "", "plain HTTP event stream that sends one event, then holds")
	injectSize := flag.String("inject-size", "", "size of the template's pool whose only rule is intercept \"inject\"; empty skips that leg")
	other := flag.String("other", "example.com", "a second HTTPS host the inject pool must splice")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	var err error
	switch {
	case *reattach != "":
		err = recheck(ctx, *addr, *token, *echo, *reattach)
	case *injectSize != "":
		err = conditional(ctx, *addr, *token, *template, sandbox.Size(*injectSize), *echo, *other)
	default:
		err = run(ctx, *addr, *token, *template, *echo, *secret, *sse)
	}
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "credsmoke:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, addr, token, template, echo, secret, sse string) error {
	stamp := strconv.FormatInt(time.Now().UnixNano(), 10)
	valueA, valueB := "key-a-"+stamp, "key-b-"+stamp

	claim := func(env map[string]sandbox.EnvVar) (*sandbox.Sandbox, error) {
		opts := []sandbox.Option{sandbox.WithNetwork(sandbox.NetNone), sandbox.WithSize(sandbox.Small)}
		if env != nil {
			opts = append(opts, sandbox.WithEnv(env))
		}
		_, sb, err := harness.Claim(ctx, addr, token, template, opts...)
		return sb, err
	}
	a, err := claim(credential(echo, valueA))
	if err != nil {
		return err
	}
	b, err := claim(credential(echo, valueB))
	if err != nil {
		return err
	}
	defer func() { _ = b.Close() }()
	plain, err := claim(nil)
	if err != nil {
		return err
	}
	defer func() { _ = plain.Close() }()
	fmt.Printf("  claimed %s (A), %s (B), %s (plain) on one intercept pool\n", a.ID, b.ID, plain.ID)

	for _, tc := range []struct {
		name      string
		sb        *sandbox.Sandbox
		want, not string
	}{
		{"A", a, valueA, valueB},
		{"B", b, valueB, valueA},
		{"plain", plain, "placeholder", "key-"},
	} {
		if err = expectHeader(ctx, tc.sb, echo, tc.want, tc.not, secret); err != nil {
			return fmt.Errorf("%s: %w", tc.name, err)
		}
	}
	fmt.Println("  each claim's origin saw only its own credential, the plain claim the guest's placeholder, all the pool secret")
	if sse != "" {
		out, _ := plain.Exec(ctx, "sh", "-c", fmt.Sprintf("timeout 4 curl -sSN --noproxy '' -x %s %s 2>&1; true", proxy, sse))
		if !strings.Contains(out, "data: one") {
			return fmt.Errorf("the first event did not reach the guest while the stream stayed open: %q", out)
		}
		fmt.Println("  an open event stream reached the guest event by event through the proxy")
	}

	leak, err := a.Exec(ctx, "sh", "-c", fmt.Sprintf("grep -rls %q /proc/[0-9]*/environ /run /etc 2>/dev/null; env | grep -c %q; true", valueA, valueA))
	if err != nil {
		return fmt.Errorf("leak probe: %w", err)
	}
	if strings.TrimSpace(leak) != "0" {
		return fmt.Errorf("the credential reached the guest: %q", leak)
	}
	env, err := a.Env(ctx)
	if err != nil {
		return fmt.Errorf("read env: %w", err)
	}
	if v := env[entry]; v.Value != "" || v.Inject == nil || v.Inject.Header != header {
		return fmt.Errorf("GET env %+v, want the inject named with the value blanked", v)
	}
	fmt.Println("  value absent from the guest (env, /proc/*/environ, /run, /etc) and blanked in GET env")

	rotated := valueA + "-r1"
	if err = a.PatchEnv(ctx, map[string]*sandbox.EnvVar{entry: new(credential(echo, rotated)[entry])}); err != nil {
		return fmt.Errorf("rotate: %w", err)
	}
	if err = expectHeader(ctx, a, echo, rotated, valueA+"\"", secret); err != nil {
		return fmt.Errorf("after a rotation: %w", err)
	}
	if err = a.Hibernate(ctx); err != nil {
		return fmt.Errorf("hibernate: %w", err)
	}
	rotated += "-r2"
	if err = a.PatchEnv(ctx, map[string]*sandbox.EnvVar{entry: new(credential(echo, rotated)[entry])}); err != nil {
		return fmt.Errorf("rotate while hibernated: %w", err)
	}
	if err = expectHeader(ctx, a, echo, rotated, "-r1\"", secret); err != nil {
		return fmt.Errorf("after a rotation while hibernated: %w", err)
	}
	fmt.Println("  rotation applied on the next request, a hibernated claim rotated without waking")

	for name, patch := range map[string]*sandbox.EnvVar{
		"an uncovered host":        {Value: "x", Guest: new(false), Inject: &sandbox.EnvInject{Hosts: []string{"example.org"}, Header: header}},
		"the pool secret's header": {Value: "x", Guest: new(false), Inject: &sandbox.EnvInject{Hosts: []string{echo}, Header: "X-Egress-Token"}},
		"a guest entry":            {Value: "x", Inject: &sandbox.EnvInject{Hosts: []string{echo}, Header: "X-Other"}},
	} {
		err = b.PatchEnv(ctx, map[string]*sandbox.EnvVar{"BAD": patch})
		if apiErr, ok := errors.AsType[*sandbox.APIError](err); !ok || apiErr.Status != http.StatusBadRequest {
			return fmt.Errorf("inject on %s: %v, want 400", name, err)
		}
	}
	if env, err = b.Env(ctx); err != nil || len(env) != 1 {
		return fmt.Errorf("env after refused patches %v %v, want only the original entry", env, err)
	}
	if err = expectPlaceholder(ctx, b, echo, "ph-"+stamp); err != nil {
		return err
	}
	fmt.Println("  a placeholder credential reached the origin only on the request that carried its placeholder")
	if err = b.SetEnv(ctx, map[string]sandbox.EnvVar{}); err != nil {
		return fmt.Errorf("clear: %w", err)
	}
	if err = expectHeader(ctx, b, echo, "placeholder", valueB, secret); err != nil {
		return fmt.Errorf("after a clear: %w", err)
	}
	fmt.Println("  uncovered host, pool secret header and guest entry refused with 400; a cleared env stops injecting")

	children, err := a.Fork(ctx, 1, time.Minute)
	if err != nil {
		return fmt.Errorf("fork: %w", err)
	}
	defer func() { _ = children[0].Close() }()
	if err = expectHeader(ctx, children[0], echo, "placeholder", rotated, secret); err != nil {
		return fmt.Errorf("fork child: %w", err)
	}
	fmt.Println("  a fork child carries no credential")
	fmt.Printf("REATTACH %s:%s:%s\n", a.ID, a.Token(), rotated)
	fmt.Println("CREDSMOKE PASS")
	return nil
}

func recheck(ctx context.Context, addr, token, echo, reattach string) error {
	parts := strings.SplitN(reattach, ":", 3)
	if len(parts) != 3 {
		return fmt.Errorf("-reattach %q, want id:token:value", reattach)
	}
	client, err := sandbox.Connect(addr, sandbox.WithAPIToken(token))
	if err != nil {
		return err
	}
	sb, err := client.Lookup(ctx, parts[0], parts[1])
	if err != nil {
		return fmt.Errorf("lookup after restart: %w", err)
	}
	defer func() { _ = sb.Close() }()
	if err = expectHeader(ctx, sb, echo, parts[2], "placeholder", ""); err != nil {
		return fmt.Errorf("after a restart: %w", err)
	}
	fmt.Println("  the credential survived a sandboxd restart")
	fmt.Println("CREDSMOKE REATTACH PASS")
	return nil
}

func conditional(ctx context.Context, addr, token, template string, size sandbox.Size, echo, other string) error {
	claim := func() (*sandbox.Sandbox, error) {
		_, sb, err := harness.Claim(ctx, addr, token, template, sandbox.WithNetwork(sandbox.NetNone), sandbox.WithSize(size))
		return sb, err
	}
	vault, err := claim()
	if err != nil {
		return err
	}
	defer func() { _ = vault.Close() }()
	bare, err := claim()
	if err != nil {
		return err
	}
	defer func() { _ = bare.Close() }()
	fmt.Printf("  claimed %s (vault) and %s (bare) on the inject pool\n", vault.ID, bare.ID)
	for _, host := range []string{echo, other} {
		if err = expectTunnel(ctx, vault, host, false); err != nil {
			return fmt.Errorf("before any credential: %w", err)
		}
	}
	fmt.Println("  with no credential both hosts splice: origin certificates, HTTP/2")

	stamp := strconv.FormatInt(time.Now().UnixNano(), 10)
	serp, fb := "serp "+stamp+"/+", "fb-"+stamp
	if err = vault.PatchEnv(ctx, map[string]*sandbox.EnvVar{
		"TS":   {Value: "2016-10-10", Guest: new(false), Inject: &sandbox.EnvInject{Hosts: []string{echo}, Query: "timestamp", Placeholder: tsPH}},
		"SERP": {Value: serp, Guest: new(false), Inject: &sandbox.EnvInject{Hosts: []string{echo}, Query: "api_key", Placeholder: serpPH}},
		"FB":   {Value: fb, Guest: new(false), Inject: &sandbox.EnvInject{Hosts: []string{echo}, Query: "access_token", Body: true, Placeholder: fbPH}},
	}); err != nil {
		return fmt.Errorf("add credentials: %w", err)
	}
	for _, tt := range []struct {
		sb          *sandbox.Sandbox
		host        string
		intercepted bool
	}{{vault, echo, true}, {vault, other, false}, {bare, echo, false}} {
		if err = expectTunnel(ctx, tt.sb, tt.host, tt.intercepted); err != nil {
			return fmt.Errorf("after the credentials: %w", err)
		}
	}
	fmt.Println("  the claim with credentials is intercepted on their host only; the other claim still splices it")

	base := "https://" + echo
	form, doc := "access_token="+fb+"&m=hi", `{"access_token":"`+fb+`"}`
	for _, tt := range []struct {
		name, args, want, not string
	}{
		{"query", "'" + base + "/time/valid?timestamp=" + url.QueryEscape(tsPH) + "'", `"valid":true`, "2016"},
		{"other query value", "'" + base + "/time/valid?timestamp=bogus'", `"valid":false`, "2016"},
		{"echoed query", "'" + base + "/get?b=2&api_key=" + url.QueryEscape(serpPH) + "&a=1'", serpPH, stamp},
		{"form body", base + "/post -d access_token=" + fbPH + " -d m=hi", fmt.Sprintf(`"content-length":"%d"`, len(form)), fb},
		{"json body", base + `/post -H 'Content-Type: application/json' -d '{"access_token":"` + fbPH + `"}'`, fmt.Sprintf(`"content-length":"%d"`, len(doc)), fb},
		{"response header echo", "-D - -o /dev/null '" + base + "/response-headers?api_key=" + url.QueryEscape(serpPH) + "'", serpPH, stamp},
	} {
		out, execErr := vault.Exec(ctx, "sh", "-c", fmt.Sprintf("curl -sS -x %s %s", proxy, tt.args))
		if execErr != nil {
			return fmt.Errorf("%s exec: %w (%s)", tt.name, execErr, out)
		}
		if !strings.Contains(out, tt.want) || strings.Contains(out, tt.not) {
			return fmt.Errorf("%s: guest saw %q, want %q and not %q", tt.name, out, tt.want, tt.not)
		}
	}
	fmt.Println("  query and form/JSON body placeholders filled at the origin; every echo of a value reached the guest as its placeholder")

	leak, err := vault.Exec(ctx, "sh", "-c", fmt.Sprintf("grep -rlsF %q /proc/[0-9]*/environ /run /etc 2>/dev/null; env | grep -cF %q; true", fb, fb))
	if err != nil || strings.TrimSpace(leak) != "0" {
		return fmt.Errorf("the credential reached the guest: %q %v", leak, err)
	}
	if err = vault.SetEnv(ctx, map[string]sandbox.EnvVar{}); err != nil {
		return fmt.Errorf("clear: %w", err)
	}
	if err = expectTunnel(ctx, vault, echo, false); err != nil {
		return fmt.Errorf("after a clear: %w", err)
	}
	fmt.Println("  value absent from the guest; a cleared env splices the next connection")
	fmt.Printf("VAULT %s\nVAULT %s\n", serp, fb)
	fmt.Println("CREDSMOKE INJECT PASS")
	return nil
}

func credential(echo, value string) map[string]sandbox.EnvVar {
	return map[string]sandbox.EnvVar{entry: {Value: value, Guest: new(false), Inject: &sandbox.EnvInject{Hosts: []string{echo}, Header: header}}}
}

func expectPlaceholder(ctx context.Context, sb *sandbox.Sandbox, echo, value string) error {
	entry := &sandbox.EnvVar{Value: value, Guest: new(false), Inject: &sandbox.EnvInject{Hosts: []string{echo}, Header: "X-Ph-Key", Placeholder: "@PH_KEY@"}}
	if err := sb.PatchEnv(ctx, map[string]*sandbox.EnvVar{"PH_KEY": entry}); err != nil {
		return fmt.Errorf("placeholder inject: %w", err)
	}
	for sent, want := range map[string]bool{"": false, "@PH_KEY@": true, "@OTHER@": false} {
		header := ""
		if sent != "" {
			header = fmt.Sprintf("-H 'X-Ph-Key: %s' ", sent)
		}
		out, err := sb.Exec(ctx, "sh", "-c", fmt.Sprintf("curl -sS -x %s %shttps://%s/headers", proxy, header, echo))
		if err != nil {
			return fmt.Errorf("placeholder exec: %w (%s)", err, out)
		}
		if strings.Contains(out, value) != want {
			return fmt.Errorf("guest sent X-Ph-Key %q: origin saw the credential %v, want %v:\n%s", sent, !want, want, out)
		}
	}
	return nil
}

// expectTunnel reads the issuer and HTTP version the guest gets from host; an intercepted tunnel shows the e2e CA and HTTP/1.1.
func expectTunnel(ctx context.Context, sb *sandbox.Sandbox, host string, intercepted bool) error {
	out, err := sb.Exec(ctx, "sh", "-c", fmt.Sprintf("curl -sSv -o /dev/null -w 'version=%%{http_version}\\n' -x %s https://%s/ 2>&1", proxy, host))
	if err != nil {
		return fmt.Errorf("%s exec: %w (%s)", host, err, out)
	}
	_, rest, found := strings.Cut(out, "issuer:")
	issuer, _, _ := strings.Cut(rest, "\n")
	mitm := strings.Contains(issuer, "e2e")
	h2 := strings.Contains(out, "version=2")
	if !found || mitm != intercepted || h2 == intercepted {
		return fmt.Errorf("%s on %s: issuer %q, HTTP/2 %t, want intercepted %t:\n%s", host, sb.ID, issuer, h2, intercepted, out)
	}
	return nil
}

func expectHeader(ctx context.Context, sb *sandbox.Sandbox, echo, want, not, secret string) error {
	out, err := sb.Exec(ctx, "sh", "-c", fmt.Sprintf("curl -sS -x %s -H '%s: placeholder' https://%s/headers", proxy, header, echo))
	if err != nil {
		return fmt.Errorf("proxied HTTPS exec: %w (%s)", err, out)
	}
	switch {
	case !strings.Contains(out, want):
		return fmt.Errorf("origin did not see %q:\n%s", want, out)
	case not != "" && strings.Contains(out, not):
		return fmt.Errorf("origin saw %q:\n%s", not, out)
	case secret != "" && !strings.Contains(out, secret):
		return fmt.Errorf("origin did not see the pool secret:\n%s", out)
	}
	return nil
}
