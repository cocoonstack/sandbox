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
	"hash/fnv"
	"io"
	"net"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	// RequiredCocoon carries the snapshot/store performance baseline.
	RequiredCocoon = "v0.5.2"

	argName       = "--name"
	argOutput     = "--output"
	argNetwork    = "--network"
	argBridge     = "--bridge"
	formatJSON    = "json"
	silkdPort     = 2048 // silkd's fixed guest vsock port, the claim-ready anchor
	egressPort    = 2049 // guest→host egress port; VMM maps it to <vsock_socket>_2049
	cmdTimeout    = 2 * time.Minute
	probeInterval = 20 * time.Millisecond
	connectMax    = 64   // "OK <port>" handshake reply cap
	infoMax       = 4096 // info response frame cap
	outputTail    = 400

	// portForwardMax caps the port_forward handshake reply line; the byte
	// stream follows on the same conn.
	portForwardMax = 4096
)

// capacitySignatures mean "this node cannot attach another VM", not "this VM
// failed"; matched as text because cocoon is a subprocess and the distinction
// survives only in its stderr. "exchange full" is EXFULL from a bridge at
// BR_MAX_PORTS — permanent until VMs are removed.
var capacitySignatures = []string{
	"exchange full",
	"no space left on device",
}

// Engine runs cocoon commands on the local node.
type Engine struct {
	bin         string
	bridges     []string
	networks    []string
	noDirectIO  bool
	restoreMode types.RestoreMode
}

// New returns a cocoon engine with node-wide network and disk policy. bridges
// and networks are the egress-lane shard lists (host bridge devices vs CNI
// conflists) egress VMs spread over; see shardOf.
func New(bin string, bridges, networks []string, noDirectIO bool, restoreMode types.RestoreMode) *Engine {
	return &Engine{bin: bin, bridges: bridges, networks: networks, noDirectIO: noDirectIO, restoreMode: restoreMode}
}

// Version reports cocoon's version string — a "vX.Y.Z" release or a
// "master-<sha>" dev build.
func (e *Engine) Version(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := e.run(ctx, "version")
	if err != nil {
		return "", err
	}
	for line := range strings.SplitSeq(string(out), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "Version:"); ok {
			return strings.TrimSpace(v), nil
		}
	}
	return "", fmt.Errorf("cocoon version: no Version line in output")
}

// VersionWarning probes cocoon and returns a non-empty warning when its
// version is below RequiredCocoon (or unreadable); version is set when known.
// A dev build (no vX.Y.Z) is assumed current.
func (e *Engine) VersionWarning(ctx context.Context) (version, warning string) {
	v, err := e.Version(ctx)
	if err != nil {
		return "", fmt.Sprintf("cannot determine cocoon version (need >= %s): %v", RequiredCocoon, err)
	}
	if below, comparable := belowFloor(v); comparable && below {
		return v, fmt.Sprintf("cocoon %s is below the required %s — upgrade cocoon", v, RequiredCocoon)
	}
	return v, ""
}

// Clone restores a VM from an exported golden directory.
func (e *Engine) Clone(ctx context.Context, fromDir, name string, key types.PoolKey) (types.VMRecord, error) {
	out, err := e.run(ctx, e.cloneArgs(fromDir, name, key)...)
	if err != nil {
		return types.VMRecord{}, err
	}
	return parseRecord(ctx, out), nil
}

// CloneSnap clones from a local-store snapshot by name — fork's fast path,
// which skips the export-to-dir copy that `Clone --from-dir` requires.
func (e *Engine) CloneSnap(ctx context.Context, snap, name string, key types.PoolKey) (types.VMRecord, error) {
	out, err := e.run(ctx, e.cloneSnapArgs(snap, name, key)...)
	if err != nil {
		return types.VMRecord{}, err
	}
	return parseRecord(ctx, out), nil
}

// RunCold boots a VM from the template image (golden builds and cache-miss
// claims), returning its lifecycle record.
func (e *Engine) RunCold(ctx context.Context, name string, key types.PoolKey) (types.VMRecord, error) {
	out, err := e.run(ctx, e.runColdArgs(name, key)...)
	if err != nil {
		return types.VMRecord{}, err
	}
	return parseRecord(ctx, out), nil
}

// Remove force-deletes a VM.
func (e *Engine) Remove(ctx context.Context, name string) error {
	_, err := e.run(ctx, "vm", "rm", "--force", name)
	return err
}

// SnapshotSave snapshots a running VM under snapName.
func (e *Engine) SnapshotSave(ctx context.Context, vmName, snapName string) error {
	_, err := e.run(ctx, "snapshot", "save", argName, snapName, vmName)
	return err
}

// Hibernate atomically snapshots a running VM under snapName and stops it,
// freeing its memory; Restore with the snapshot resumes it.
func (e *Engine) Hibernate(ctx context.Context, vmName, snapName string) error {
	_, err := e.run(ctx, "vm", "hibernate", argName, snapName, vmName)
	return err
}

// Restore resumes a VM from a snapshot with its memory state and identity
// intact (cocoon reseeds entropy only on restore), returning its vsock UDS.
func (e *Engine) Restore(ctx context.Context, vmName, snapRef string) (string, error) {
	out, err := e.run(ctx, e.restoreCmdArgs(vmName, snapRef)...)
	if err != nil {
		return "", err
	}
	return parseRecord(ctx, out).VsockSocket, nil
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
	out, err := e.run(ctx, "snapshot", "list", "--format", formatJSON)
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
	args := append([]string{"vm", "list", "--format", formatJSON}, filters...)
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

// DialGuestPort opens a raw byte stream to 127.0.0.1:port inside the guest
// by driving silkd's port_forward handshake, then returns the connection
// carrying the relayed TCP bytes — the preview server proxies HTTP over it.
func (e *Engine) DialGuestPort(ctx context.Context, vsockSocket string, port uint16) (net.Conn, error) {
	conn, err := e.DialSilkd(ctx, vsockSocket)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	req, err := wire.EncodeRequest(wire.PortForward{Port: port})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if _, err := conn.Write(append(req, '\n')); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write port_forward: %w", err)
	}
	line, readErr := readLine(conn, portForwardMax)
	if readErr != nil {
		_ = conn.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("read port_forward reply: %w", readErr)
	}
	resp, parseErr := wire.DecodeResponse([]byte(line))
	if parseErr != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("parse port_forward reply: %w", parseErr)
	}
	if _, ok := resp.(*wire.Ready); !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("port_forward %d: %s", port, respFail(resp))
	}
	return newGuestPortConn(conn), nil
}

func (e *Engine) cloneArgs(fromDir, name string, key types.PoolKey) []string {
	// --pull: a checkpoint/template export carries only COW + memory; on a
	// cross-node claim the base image blobs resolve locally or are pulled
	// by digest.
	args := []string{"vm", "clone", "--from-dir", fromDir, argName, name, "--pull", argOutput, formatJSON, e.directIOArg()}
	args = append(args, e.restoreArgs()...)
	return append(args, e.netArgs(name, key, false)...)
}

func (e *Engine) cloneSnapArgs(snap, name string, key types.PoolKey) []string {
	args := []string{"vm", "clone", snap, argName, name, "--pull", argOutput, formatJSON, e.directIOArg()}
	args = append(args, e.restoreArgs()...)
	return append(args, e.netArgs(name, key, false)...)
}

func (e *Engine) restoreCmdArgs(vmName, snapRef string) []string {
	args := append([]string{"vm", "restore", argOutput, formatJSON}, e.restoreArgs()...)
	return append(args, vmName, snapRef)
}

func (e *Engine) directIOArg() string {
	return "--no-direct-io=" + strconv.FormatBool(e.noDirectIO)
}

func (e *Engine) restoreArgs() []string {
	if e.restoreMode == "" {
		return nil
	}
	return []string{"--restore-mode", string(e.restoreMode)}
}

func (e *Engine) runColdArgs(name string, key types.PoolKey) []string {
	spec, _ := key.Size.Spec()
	args := []string{"vm", "run", argName, name, argOutput, formatJSON, "--cpu", strconv.Itoa(spec.CPU), "--memory", spec.Memory, e.directIOArg()}
	if key.Engine == types.EngineFC {
		// Firecracker is a per-pool cold-boot choice; clones inherit the
		// hypervisor from the golden's pinned snapshot, so only RunCold flags it.
		args = append(args, "--fc")
	}
	args = append(args, e.netArgs(name, key, true)...)
	return append(args, key.Template)
}

func (e *Engine) netArgs(name string, key types.PoolKey, cold bool) []string {
	if key.Net == types.NetNone {
		if cold {
			return []string{"--nics", "0"}
		}
		return nil
	}
	if len(e.networks) > 0 {
		return []string{argNetwork, shardOf(e.networks, name)}
	}
	return []string{argBridge, shardOf(e.bridges, name)}
}

func (e *Engine) run(ctx context.Context, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, cmdTimeout)
	defer cancel()
	log.WithFunc("engine.run").Debugf(ctx, "cocoon %s", strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, e.bin, args...) //nolint:gosec // bin comes from node config, args are built internally
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("cocoon %s: %w: %s", strings.Join(args[:min(len(args), 2)], " "), err, tail(stderr.String()))
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
	probe, err := wire.EncodeRequest(wire.Info{})
	if err != nil {
		return fmt.Errorf("encode info: %w", err)
	}
	if _, err = conn.Write(append(probe, '\n')); err != nil {
		return fmt.Errorf("write info: %w", err)
	}
	// Unlike the handshake reader, buffered over-read is safe here: the conn
	// is discarded after this one reply.
	reply, err := bufio.NewReader(io.LimitReader(conn, infoMax)).ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("read info reply: %w", err)
	}
	resp, err := wire.DecodeResponse(reply)
	if err != nil {
		return fmt.Errorf("parse info reply: %w", err)
	}
	if _, ok := resp.(*wire.InfoResp); !ok {
		return fmt.Errorf("info reply type %q", resp.RespType())
	}
	return nil
}

// EgressSocketPath is the host UDS the VMM connects when the guest dials
// CID2:egressPort — sandboxd listens here to serve the egress proxy.
func EgressSocketPath(vsockSocket string) string {
	return fmt.Sprintf("%s_%d", vsockSocket, egressPort)
}

// shardOf picks a shard by hashing the VM name, not by counter: the record
// persists the attachment a VM was built on, so the choice must be
// reproducible without process state.
func shardOf(shards []string, name string) string {
	if len(shards) == 0 {
		return ""
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	// >>1 keeps the index conversion positive even where int is 32-bit.
	return shards[int(h.Sum32()>>1)%len(shards)]
}

// respFail renders a non-success reply: the error frame's own text, or the
// unexpected frame's type.
func respFail(resp wire.Response) string {
	if errResp, ok := resp.(*wire.ErrorResp); ok {
		return errResp.Error()
	}
	return "unexpected frame " + resp.RespType()
}

// parseRecord reads a lifecycle command's --output json VM record; best-effort,
// so an unparseable record yields the zero value (callers fall back to vm list).
func parseRecord(ctx context.Context, out []byte) types.VMRecord {
	var rec types.VMRecord
	if err := json.Unmarshal(out, &rec); err != nil {
		log.WithFunc("engine.parseRecord").Warnf(ctx, "parse vm record, will poll for vsock: %v", err)
		return types.VMRecord{}
	}
	return rec
}

// belowFloor reports whether cocoon version v is below RequiredCocoon;
// comparable is false for a non-release build (no vX.Y.Z), assumed current.
func belowFloor(v string) (below, comparable bool) {
	cur, ok := parseSemver(v)
	if !ok {
		return false, false
	}
	floor, _ := parseSemver(RequiredCocoon)
	return slices.Compare(cur[:], floor[:]) < 0, true
}

func parseSemver(s string) ([3]int, bool) {
	s = strings.TrimPrefix(s, "v")
	if i := strings.IndexAny(s, "-+"); i >= 0 {
		s = s[:i]
	}
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return [3]int{}, false
	}
	var out [3]int
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return [3]int{}, false
		}
		out[i] = n
	}
	return out, true
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

// CapacitySignature returns the node-capacity signature err carries, or "".
func CapacitySignature(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	for _, sig := range capacitySignatures {
		if strings.Contains(msg, sig) {
			return sig
		}
	}
	return ""
}

func tail(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= outputTail {
		return s
	}
	return "..." + s[len(s)-outputTail:]
}
