package sandbox

import (
	"bufio"
	"io"
	"testing"
)

func TestPtyEchoAndExit(t *testing.T) {
	sb := fakeSandbox(t)
	pty, err := sb.OpenPty(t.Context(), PtyOpts{Cols: 80, Rows: 24})
	if err != nil {
		t.Fatalf("OpenPty: %v", err)
	}
	if pty.PID == 0 {
		t.Fatal("no pid")
	}
	defer pty.Close()

	// The fake echoes input; write a line and read it back.
	if _, err = pty.Write([]byte("ping\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	line, err := bufio.NewReader(pty).ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if line != "ping\n" {
		t.Errorf("read %q, want ping", line)
	}

	// Resize is a separate RPC.
	if err := pty.Resize(t.Context(), 120, 40); err != nil {
		t.Fatalf("resize: %v", err)
	}

	// "exit" ends the shell → Read returns EOF and ExitCode is set.
	if _, err := pty.Write([]byte("exit\n")); err != nil {
		t.Fatalf("write exit: %v", err)
	}
	if _, err := io.ReadAll(pty); err != nil {
		t.Fatalf("drain to EOF: %v", err)
	}
	code, ok := pty.ExitCode()
	if !ok || code != 0 {
		t.Errorf("exit code %d ok=%v, want 0 true", code, ok)
	}
}
