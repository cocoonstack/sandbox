// Package outboundtest holds the fixtures tests of a sandbox's egress doors share.
package outboundtest

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// TapLog records nft lock calls in place of netfilter; its Lock and Unlock always succeed.
type TapLog struct {
	mu      sync.Mutex
	locks   []string
	unlocks []string
}

func (l *TapLog) Lock(tap string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.locks = append(l.locks, tap)
	return nil
}

func (l *TapLog) Unlock(tap string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.unlocks = append(l.unlocks, tap)
	return nil
}

func (l *TapLog) Locked() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.locks)
}

func (l *TapLog) Unlocked() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.unlocks)
}

// SockRoot is a directory under /tmp, short enough for unix socket paths.
func SockRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "eg")
	if err != nil {
		t.Fatalf("sockdir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func VsockSandbox(t testing.TB, id string, key types.PoolKey) *types.Sandbox {
	t.Helper()
	return &types.Sandbox{ID: id, Key: key, VsockSocket: filepath.Join(SockRoot(t), "v")}
}

func EgressClient(path string) *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyURL(&url.URL{Scheme: "http", Host: "proxy.internal:3128"}),
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", path)
			},
		},
	}
}

// ConnectDoor opens a CONNECT tunnel to target through the door at path and returns the connection and the tunnel's body.
func ConnectDoor(t testing.TB, path, target string) (net.Conn, io.ReadCloser) {
	t.Helper()
	conn, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		t.Fatalf("dial the door: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target); err != nil {
		t.Fatalf("send CONNECT: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read the CONNECT answer: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT answered %s", resp.Status)
	}
	return conn, resp.Body
}

func Hostname(t testing.TB, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u.Hostname()
}
