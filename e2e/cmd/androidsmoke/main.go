// androidsmoke is the android-flavor acceptance: claim (none/xlarge) → Android
// init tree over the relay → adb CNXN handshake on the adb port →
// checkpoint/branch of the booted guest. No session step: the guest
// ships no bash.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/e2e/internal/harness"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

const (
	adbPort    = 5555
	adbCnxn    = 0x4e584e43
	adbVersion = 0x01000001
	adbMaxData = 256 * 1024
	// adbd is a core service, up well before the framework completes.
	adbWait = 2 * time.Minute
	// silkd's base PATH does not carry the Android userspace.
	shellPath = "PATH=/system/bin:/system/xbin:$PATH"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "ghcr.io/cocoonstack/sandbox/android:15", "android template ref")
	flag.Parse()

	if err := run(*addr, *token, *template); err != nil {
		fmt.Fprintln(os.Stderr, "androidsmoke:", err)
		os.Exit(1)
	}
	fmt.Println("ANDROIDSMOKE PASS")
}

func run(addr, token, template string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	start := time.Now()
	_, sb, err := harness.Claim(ctx, addr, token, template,
		sandbox.WithNetwork(sandbox.NetNone), sandbox.WithSize(sandbox.XLarge),
		sandbox.WithTimeout(30*time.Minute))
	if err != nil {
		return err
	}
	defer func() { _ = sb.Close() }()
	fmt.Printf("  claim: android xlarge up in %.1fs (silkd probed)\n", time.Since(start).Seconds())

	if treeErr := initTree(ctx, sb); treeErr != nil {
		return treeErr
	}
	if adbErr := adbHandshake(ctx, sb); adbErr != nil {
		return adbErr
	}

	ckpt, err := sb.Checkpoint(ctx, "android-booted")
	if err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	defer func() { _ = ckpt.Delete(ctx) }()
	branch, err := ckpt.New(ctx)
	if err != nil {
		return fmt.Errorf("branch: %w", err)
	}
	defer func() { _ = branch.Close() }()
	if err := initTree(ctx, branch); err != nil {
		return fmt.Errorf("branch: %w", err)
	}
	if err := adbHandshake(ctx, branch); err != nil {
		return fmt.Errorf("branch: %w", err)
	}
	fmt.Println("  checkpoint: branch of the booted guest answers ps + adb")
	return nil
}

// The framework (zygote, system_server) starts minutes after init on a
// nested first boot; silkd answers long before.
func initTree(ctx context.Context, sb *sandbox.Sandbox) error {
	deadline := time.Now().Add(5 * time.Minute)
	for {
		out, err := sb.Exec(ctx, "/system/bin/sh", "-c", shellPath+"; ps -A")
		if err != nil {
			return fmt.Errorf("exec ps: %w", err)
		}
		if strings.Contains(out, "zygote") && strings.Contains(out, "system_server") {
			fmt.Println("  exec: ps -A shows the Android init tree (zygote, system_server)")
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("framework never came up:\n%s", head(out))
		}
		time.Sleep(3 * time.Second)
	}
}

func adbHandshake(ctx context.Context, sb *sandbox.Sandbox) error {
	deadline := time.Now().Add(adbWait)
	for {
		reply, err := dialAdb(ctx, sb)
		if err == nil {
			fmt.Printf("  adb: port %d answered %s to CNXN\n", adbPort, reply)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("adb port %d: %w", adbPort, err)
		}
		time.Sleep(2 * time.Second)
	}
}

// dialAdb sends one ADB CNXN message; adbd replies CNXN (unauthenticated)
// or AUTH — either proves a live endpoint.
func dialAdb(ctx context.Context, sb *sandbox.Sandbox) (string, error) {
	pc, err := sb.DialPort(ctx, adbPort)
	if err != nil {
		return "", err
	}
	defer func() { _ = pc.Close() }()
	payload := []byte("host::")
	var sum uint32
	for _, b := range payload {
		sum += uint32(b)
	}
	msg := make([]byte, 24, 24+len(payload))
	binary.LittleEndian.PutUint32(msg[0:], adbCnxn)
	binary.LittleEndian.PutUint32(msg[4:], adbVersion)
	binary.LittleEndian.PutUint32(msg[8:], adbMaxData)
	binary.LittleEndian.PutUint32(msg[12:], uint32(len(payload))) //nolint:gosec // fixed 6-byte payload
	binary.LittleEndian.PutUint32(msg[16:], sum)
	binary.LittleEndian.PutUint32(msg[20:], adbCnxn^0xffffffff)
	if _, err := pc.Write(append(msg, payload...)); err != nil {
		return "", err
	}
	resp := make([]byte, 24)
	if _, err := io.ReadFull(pc, resp); err != nil {
		return "", err
	}
	reply := string(resp[:4])
	if reply != "CNXN" && reply != "AUTH" {
		return "", fmt.Errorf("unexpected reply %q", reply)
	}
	return reply, nil
}

func head(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > 12 {
		lines = lines[:12]
	}
	return strings.Join(lines, "\n")
}
