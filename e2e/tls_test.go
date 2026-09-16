package e2e

import (
	"bytes"
	"cmp"
	"crypto/tls"
	"crypto/x509"
	"encoding/json/v2"
	"encoding/pem"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/server"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

func TestCaddyTLSCluster(t *testing.T) {
	bin := os.Getenv("CADDY_BIN")
	if bin == "" {
		t.Skip("set CADDY_BIN to run the TLS edge integration")
	}
	dir := t.TempDir()
	cert, key, roots := edgeCertificate(t, dir)
	la, lb := reserveEdgePort(t), reserveEdgePort(t)
	a, b := la.Addr().String(), lb.Addr().String()
	owner := "https://" + b
	entry := "https://" + a
	st := startStack(t, "node-token")
	ownerServer := server.New(st.token, nil, owner, st.mgr, st.eng.real, nil, nil, nil, nil)
	backend := httptest.NewServer(ownerServer.Handler())
	t.Cleanup(func() { backend.Close(); ownerServer.CloseRelays() })
	origin := startStack(t, "node-token")
	placer := &edgePlacer{internal: "owner.invalid:7777", public: owner}
	entryServer := server.New(origin.token, nil, entry, origin.mgr, origin.eng.real, placer, nil, nil, nil)
	front := httptest.NewServer(entryServer.Handler())
	t.Cleanup(func() { front.Close(); entryServer.CloseRelays() })
	servers := map[string]any{}
	for listen, upstream := range map[string]string{a: front.Listener.Addr().String(), b: backend.Listener.Addr().String()} {
		servers[listen] = map[string]any{
			"listen": []string{listen}, "tls_connection_policies": []any{map[string]any{}},
			"automatic_https": map[string]bool{"disable": true},
			"routes": []any{map[string]any{"handle": []any{map[string]any{
				"handler": "reverse_proxy", "upstreams": []any{map[string]string{"dial": upstream}},
				"transport": map[string]any{"protocol": "http", "versions": []string{"1.1"}},
			}}}},
		}
	}
	config, err := json.Marshal(map[string]any{
		"admin": map[string]bool{"disabled": true},
		"apps": map[string]any{
			"tls":  map[string]any{"certificates": map[string]any{"load_files": []any{map[string]string{"certificate": cert, "key": key}}}},
			"http": map[string]any{"servers": servers},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "caddy.json")
	if err = os.WriteFile(configPath, config, 0o600); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	cmd := exec.CommandContext(t.Context(), bin, "run", "--config", configPath)
	cmd.Env = append(os.Environ(), "XDG_DATA_HOME="+dir, "XDG_CONFIG_HOME="+dir)
	cmd.Stdout, cmd.Stderr = &logs, &logs
	_, _ = la.Close(), lb.Close()
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Log(logs.String())
		}
	})
	client, err := sandbox.Connect(entry, sandbox.WithAPIToken(st.token), sandbox.WithTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, err := client.Info(t.Context()); return err == nil })
	t.Run("Go", func(t *testing.T) { checkTLSClient(t, client, owner) })
	t.Run("Python", func(t *testing.T) {
		python := cmp.Or(os.Getenv("PYTHON_BIN"), "python3")
		cmd := exec.CommandContext(t.Context(), python, "tls_client.py", entry, owner, cert)
		cmd.Env = append(os.Environ(), "PYTHONPATH=../sdk/python")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("Python TLS flow: %v\n%s", err, out)
		}
	})
}

func checkTLSClient(t *testing.T, client *sandbox.Client, owner string) {
	t.Helper()
	sb, err := client.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Close()
	if sb.Owner() != owner {
		t.Fatalf("owner %q, want %q", sb.Owner(), owner)
	}
	if out, execErr := sb.Exec(t.Context(), "echo", "tls"); execErr != nil || out != "tls\n" {
		t.Fatalf("exec = %q, %v", out, execErr)
	}
	recovered, err := client.Lookup(t.Context(), sb.ID, sb.Token())
	if err != nil || recovered.Owner() != owner {
		t.Fatalf("lookup = %v, %v", recovered, err)
	}
	pc, err := sb.DialPort(t.Context(), 5000)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	if _, err = pc.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err = pc.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if out, readErr := io.ReadAll(pc); readErr != nil || string(out) != "tail" {
		t.Fatalf("half-close tail = %q, %v", out, readErr)
	}
	l, err := sb.ProxyPort(t.Context(), "127.0.0.1:0", 5000)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	conn, err := net.DialTimeout("tcp", l.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.WriteString(conn, "proxy")
	_ = conn.(*net.TCPConn).CloseWrite()
	if out, readErr := io.ReadAll(conn); readErr != nil || string(out) != "proxy" {
		t.Fatalf("proxy = %q, %v", out, readErr)
	}
	children, err := sb.Fork(t.Context(), 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer children[0].Close()
	if children[0].Owner() != owner {
		t.Fatalf("fork owner = %q", children[0].Owner())
	}
	if err := sb.Close(); err != nil {
		t.Fatal(err)
	}
}

func edgeCertificate(t *testing.T, dir string) (string, string, *x509.CertPool) {
	t.Helper()
	fixture := httptest.NewTLSServer(nil)
	defer fixture.Close()
	cert := fixture.TLS.Certificates[0]
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	for path, block := range map[string]*pem.Block{
		certPath: {Type: "CERTIFICATE", Bytes: cert.Certificate[0]},
		keyPath:  {Type: "PRIVATE KEY", Bytes: key},
	} {
		if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	roots := x509.NewCertPool()
	roots.AddCert(fixture.Certificate())
	return certPath, keyPath, roots
}

func reserveEdgePort(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

type edgePlacer struct {
	internal string
	public   string
}

func (p *edgePlacer) ClientAddr(addr string) string {
	if addr == p.internal {
		return p.public
	}
	return addr
}

func (p *edgePlacer) Candidates(string) []string                     { return []string{p.internal} }
func (p *edgePlacer) VolumeCandidates(string, []string) []string     { return nil }
func (p *edgePlacer) TemplateOwners(string) []string                 { return nil }
func (p *edgePlacer) VolumeOwners([]string) []string                 { return nil }
func (p *edgePlacer) TemplateVolumeOwners(string, []string) []string { return nil }
func (p *edgePlacer) VolumeHolders() map[string]int                  { return nil }
func (p *edgePlacer) PeerAddrs() []string                            { return []string{p.internal} }
func (p *edgePlacer) ConfigMismatches() int                          { return 0 }
