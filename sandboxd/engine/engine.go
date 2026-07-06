// Package engine drives VM lifecycle through the cocoon CLI and dials the
// in-guest silkd over hybrid vsock.
//
// cocoon runs as a subprocess deliberately: the CLI is cocoon's stable
// contract (it exports no lifecycle library), it is the exact interface every
// latency figure was measured through, and no lifecycle call sits on the
// warm-claim path.
package engine

import (
	"bufio"
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

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	silkdPort     = 2048 // silkd's fixed guest vsock port, the claim-ready anchor
	cmdTimeout    = 2 * time.Minute
	probeInterval = 20 * time.Millisecond
	connectMax    = 64   // "OK <port>" handshake reply cap
	infoMax       = 4096 // info response frame cap
	outputTail    = 400
)

var infoProbe = []byte(`{"v":1,"op":"info"}` + "\n")

// portForwardMax caps the port_forward handshake reply line; the byte
// stream follows on the same conn.
const portForwardMax = 4096

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

// Remove kills and removes a VM. Stop is an immediate `--force` kill — rm's
// own stop-before-delete waits a 30s graceful window sandbox guests never
// answer (no CtrlAltDel path), which would put 30s on every release and
// reap. The stop error is ignored: the VM may already be stopped or gone;
// rm is authoritative.
func (e *Engine) Remove(ctx context.Context, name string) error {
	_, _ = e.run(ctx, "vm", "stop", "--force", name)
	_, err := e.run(ctx, "vm", "rm", "--force", name)
	return err
}

// SnapshotSave snapshots a running VM under snapName.
func (e *Engine) SnapshotSave(ctx context.Context, vmName, snapName string) error {
	_, err := e.run(ctx, "snapshot", "save", "--name", snapName, vmName)
	return err
}

// Hibernate atomically snapshots a running VM under snapName and stops it,
// freeing its memory; Restore with the snapshot resumes it.
func (e *Engine) Hibernate(ctx context.Context, vmName, snapName string) error {
	_, err := e.run(ctx, "vm", "hibernate", "--name", snapName, vmName)
	return err
}

// Restore resumes a VM from a snapshot with its memory state and identity
// intact (cocoon reseeds entropy only on restore).
func (e *Engine) Restore(ctx context.Context, vmName, snapRef string) error {
	_, err := e.run(ctx, "vm", "restore", vmName, snapRef)
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

// SnapshotList returns the names of all snapshots in cocoon's local DB.
func (e *Engine) SnapshotList(ctx context.Context) ([]string, error) {
	out, err := e.run(ctx, "snapshot", "list", "--format", "json")
	if err != nil {
		return nil, err
	}
	// An empty store prints a human line ("No snapshots found."), not JSON.
	out = bytes.TrimSpace(out)
	if len(out) == 0 || out[0] != '[' {
		return nil, nil
	}
	var snaps []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(out, &snaps); err != nil {
		return nil, fmt.Errorf("parse snapshot list: %w", err)
	}
	names := make([]string, 0, len(snaps))
	for _, s := range snaps {
		if s.Name != "" {
			names = append(names, s.Name)
		}
	}
	return names, nil
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

// DialSilkd connects to a VM's silkd through the hybrid-vsock UDS:
// dial, send "CONNECT <port>", expect an "OK" reply.
func (e *Engine) DialSilkd(ctx context.Context, vsockSocket string) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", vsockSocket)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if _, err = fmt.Fprintf(conn, "CONNECT %d\n", silkdPort); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write CONNECT: %w", err)
	}
	reply, err := readLine(conn, connectMax)
	if err != nil {
		_ = conn.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("read CONNECT reply: %w", err)
	}
	if !strings.HasPrefix(reply, "OK ") {
		_ = conn.Close()
		return nil, fmt.Errorf("hybrid vsock CONNECT %d: %s", silkdPort, strings.TrimSpace(reply))
	}
	return conn, nil
}

// Probe polls until silkd completes an info round-trip — the claim-ready
// signal (probe what the product uses, not cocoon-agent).
func (e *Engine) Probe(ctx context.Context, vsockSocket string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastErr error
	for {
		if lastErr = e.infoRoundTrip(ctx, vsockSocket); lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("silkd probe: %w (last: %v)", ctx.Err(), lastErr)
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

func (e *Engine) infoRoundTrip(ctx context.Context, vsockSocket string) error {
	conn, err := e.DialSilkd(ctx, vsockSocket)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if _, err = conn.Write(infoProbe); err != nil {
		return fmt.Errorf("write info: %w", err)
	}
	// Unlike the handshake reader, buffered over-read is safe here: the conn
	// is discarded after this one reply.
	reply, err := bufio.NewReader(io.LimitReader(conn, infoMax)).ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("read info reply: %w", err)
	}
	var frame struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(reply, &frame); err != nil {
		return fmt.Errorf("parse info reply: %w", err)
	}
	if frame.Type != "info" {
		return fmt.Errorf("info reply type %q", frame.Type)
	}
	return nil
}

// DialGuestPort opens a raw byte stream to 127.0.0.1:port inside the guest
// by driving silkd's port_forward handshake, then returns the connection
// carrying the relayed TCP bytes — the preview server proxies HTTP over it.
func (e *Engine) DialGuestPort(ctx context.Context, vsockSocket string, port uint16) (net.Conn, error) {
	conn, err := e.DialSilkd(ctx, vsockSocket)
	if err != nil {
		return nil, err
	}
	req := fmt.Sprintf(`{"v":1,"op":"port_forward","port":%d}`+"\n", port)
	if _, err := conn.Write([]byte(req)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write port_forward: %w", err)
	}
	line, readErr := readLine(conn, portForwardMax)
	if readErr != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("read port_forward reply: %w", readErr)
	}
	var frame struct {
		Type    string `json:"type"`
		Kind    string `json:"kind"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(line), &frame); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("parse port_forward reply: %w", err)
	}
	if frame.Type != "ready" {
		_ = conn.Close()
		return nil, fmt.Errorf("port_forward %d: %s: %s", port, frame.Kind, frame.Message)
	}
	return conn, nil
}

// readLine reads byte-wise so nothing past the newline is consumed —
// the same conn carries the silkd protocol right after the handshake.
func readLine(conn net.Conn, max int) (string, error) {
	var sb strings.Builder
	var b [1]byte
	for sb.Len() < max {
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			return "", err
		}
		if b[0] == '\n' {
			return sb.String(), nil
		}
		sb.WriteByte(b[0]) //nolint:gosec // G602 false positive on [1]byte in gosec ≤ v2.9.0
	}
	return "", fmt.Errorf("reply exceeds %d bytes", max)
}

func tail(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= outputTail {
		return s
	}
	return "..." + s[len(s)-outputTail:]
}
