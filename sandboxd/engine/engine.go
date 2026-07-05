// Package engine drives VM lifecycle through the cocoon CLI and dials the
// in-guest agent over hybrid vsock.
//
// cocoon runs as a subprocess deliberately: the CLI is cocoon's stable
// contract (it exports no lifecycle library), it is the exact interface every
// latency figure was measured through, and no lifecycle call sits on the
// warm-claim path.
package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"time"

	agentclient "github.com/cocoonstack/cocoon-agent/client"
	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	vsockAgentPort = 1024 // cocoon-agent's fixed vsock port
	cmdTimeout     = 2 * time.Minute
	probeInterval  = 20 * time.Millisecond
	replyMax       = 64
	outputTail     = 400
)

// Engine runs cocoon commands on the local node.
type Engine struct {
	bin     string
	bridge  string
	network string
}

// New returns an Engine invoking bin; bridge or network picks the
// egress-lane attachment.
func New(bin, bridge, network string) *Engine {
	return &Engine{bin: bin, bridge: bridge, network: network}
}

// Clone restores a VM from an exported golden directory. The no-network lane
// passes no net flags at all — the golden has no NIC to retarget, the only
// clone shape FC supports; the egress lane re-attaches to the node's bridge
// or CNI network. cocoon signals the in-guest reseed itself after resume.
func (e *Engine) Clone(ctx context.Context, fromDir, name string, key types.PoolKey) error {
	_, err := e.run(ctx, e.cloneArgs(fromDir, name, key)...)
	return err
}

// RunCold boots a VM from the template image (golden builds and cache-miss
// claims).
func (e *Engine) RunCold(ctx context.Context, name string, key types.PoolKey) error {
	args, err := e.runColdArgs(name, key)
	if err != nil {
		return err
	}
	_, err = e.run(ctx, args...)
	return err
}

// Remove force-removes a VM.
func (e *Engine) Remove(ctx context.Context, name string) error {
	_, err := e.run(ctx, "vm", "rm", "--force", name)
	return err
}

// SnapshotSave snapshots a running VM under snapName.
func (e *Engine) SnapshotSave(ctx context.Context, vmName, snapName string) error {
	_, err := e.run(ctx, "snapshot", "save", "--name", snapName, vmName)
	return err
}

// SnapshotExport exports a snapshot into toDir (cocoon requires it absent or
// empty); the result pairs with `vm clone --from-dir`.
func (e *Engine) SnapshotExport(ctx context.Context, snapName, toDir string) error {
	_, err := e.run(ctx, "snapshot", "export", snapName, "--to-dir", toDir)
	return err
}

// SnapshotRemove deletes a snapshot from cocoon's local snapshot DB.
func (e *Engine) SnapshotRemove(ctx context.Context, snapName string) error {
	_, err := e.run(ctx, "snapshot", "rm", snapName)
	return err
}

// List returns cocoon's view of local VMs, optionally filtered by name.
func (e *Engine) List(ctx context.Context, filters ...string) ([]types.VMRecord, error) {
	args := append([]string{"vm", "list", "--format", "json"}, filters...)
	out, err := e.run(ctx, args...)
	if err != nil {
		return nil, err
	}
	var vms []types.VMRecord
	if err := json.Unmarshal(out, &vms); err != nil {
		return nil, fmt.Errorf("parse vm list: %w", err)
	}
	return vms, nil
}

// DialAgent connects to a VM's cocoon-agent through the hybrid-vsock UDS:
// dial, send "CONNECT <port>", expect an "OK" reply.
func (e *Engine) DialAgent(ctx context.Context, vsockSocket string) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", vsockSocket)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", vsockAgentPort); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write CONNECT: %w", err)
	}
	reply, err := readReplyLine(conn)
	if err != nil {
		_ = conn.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("read CONNECT reply: %w", err)
	}
	if !strings.HasPrefix(reply, "OK ") {
		_ = conn.Close()
		return nil, fmt.Errorf("hybrid vsock CONNECT %d: %s", vsockAgentPort, strings.TrimSpace(reply))
	}
	return conn, nil
}

// Probe polls until the agent completes an end-to-end exec of true — the
// sandbox readiness signal.
func (e *Engine) Probe(ctx context.Context, vsockSocket string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastErr error
	for {
		if err := e.execTrue(ctx, vsockSocket); err == nil {
			return nil
		} else { //nolint:revive // lastErr must survive the loop for the timeout message
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("agent probe: %w (last: %v)", ctx.Err(), lastErr)
		case <-time.After(probeInterval):
		}
	}
}

func (e *Engine) cloneArgs(fromDir, name string, key types.PoolKey) []string {
	args := []string{"vm", "clone", "--from-dir", fromDir, "--name", name}
	return append(args, e.netArgs(key, false)...)
}

func (e *Engine) runColdArgs(name string, key types.PoolKey) ([]string, error) {
	spec, ok := key.Size.Spec()
	if !ok {
		return nil, fmt.Errorf("unknown size %q", key.Size)
	}
	args := []string{"vm", "run", "--name", name, "--cpu", strconv.Itoa(spec.CPU), "--memory", spec.Memory}
	if key.Backend() == types.BackendFC {
		args = append(args, "--fc")
	}
	args = append(args, e.netArgs(key, true)...)
	return append(args, key.Template), nil
}

func (e *Engine) netArgs(key types.PoolKey, cold bool) []string {
	if key.Net == types.NetNone {
		if cold {
			return []string{"--nics", "0"}
		}
		return nil
	}
	if e.network != "" {
		return []string{"--network", e.network}
	}
	return []string{"--bridge", e.bridge}
}

func (e *Engine) run(ctx context.Context, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, cmdTimeout)
	defer cancel()
	log.WithFunc("engine.run").Debugf(ctx, "cocoon %s", strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, e.bin, args...) //nolint:gosec // bin comes from node config, args are built internally
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("cocoon %s: %w: %s", strings.Join(args[:2], " "), err, tail(stderr.String()))
	}
	return stdout.Bytes(), nil
}

func (e *Engine) execTrue(ctx context.Context, vsockSocket string) error {
	conn, err := e.DialAgent(ctx, vsockSocket)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	code, err := agentclient.Run(ctx, conn, []string{"true"}, nil, strings.NewReader(""), io.Discard, io.Discard)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("probe exec exit %d", code)
	}
	return nil
}

// readReplyLine reads byte-wise so nothing past the newline is consumed —
// the same conn carries the agent protocol right after the handshake.
func readReplyLine(conn net.Conn) (string, error) {
	var sb strings.Builder
	buf := make([]byte, 1)
	for sb.Len() < replyMax {
		if _, err := io.ReadFull(conn, buf); err != nil {
			return "", err
		}
		if buf[0] == '\n' {
			return sb.String(), nil
		}
		sb.WriteByte(buf[0])
	}
	return "", fmt.Errorf("reply exceeds %d bytes", replyMax)
}

func tail(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= outputTail {
		return s
	}
	return "..." + s[len(s)-outputTail:]
}
