package engine

import (
	"strings"
	"testing"
)

func TestInstallCACertWritesCertAndUpdates(t *testing.T) {
	path := sockPath(t)
	fake := serveFakeSilkd(t, path)
	if err := newTestEngine().InstallCACert(t.Context(), path, []byte("CERT-PEM")); err != nil {
		t.Fatalf("InstallCACert: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.writePath != caCertGuestPath {
		t.Errorf("cert path = %q, want %q", fake.writePath, caCertGuestPath)
	}
	if fake.writeMode != 0o644 {
		t.Errorf("cert mode = %o, want 644", fake.writeMode)
	}
	if string(fake.writeData) != "CERT-PEM" {
		t.Errorf("cert data = %q, want CERT-PEM", fake.writeData)
	}
	if len(fake.execArgv) < 3 || fake.execArgv[0] != "sh" {
		t.Fatalf("exec argv = %v, want an sh -c invocation", fake.execArgv)
	}
	cmd := fake.execArgv[len(fake.execArgv)-1]
	if !strings.Contains(cmd, caCertGuestPath) || !strings.Contains(cmd, caBundlePath) {
		t.Errorf("exec appends %q, want it to cat %s into %s", cmd, caCertGuestPath, caBundlePath)
	}
	if !strings.Contains(cmd, "&& sync "+caCertGuestPath+" "+caBundlePath) {
		t.Errorf("exec %q does not sync the cert and bundle, so a cold boot inside the writeback window loses them", cmd)
	}
	if fake.execEnv["PATH"] == "" {
		t.Error("exec env has no PATH; the guest command is run with an empty env")
	}
}

func TestInstallCACertNonzeroExitFails(t *testing.T) {
	path := sockPath(t)
	fake := serveFakeSilkd(t, path)
	fake.set(func(f *fakeSilkd) { f.execCode = 3 })
	err := newTestEngine().InstallCACert(t.Context(), path, []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "exit code 3") {
		t.Errorf("got %v, want exit code 3 failure", err)
	}
}

func TestInstallCACertWriteErrorFrameFails(t *testing.T) {
	path := sockPath(t)
	fake := serveFakeSilkd(t, path)
	fake.set(func(f *fakeSilkd) { f.writeErr = "disk full" })
	err := newTestEngine().InstallCACert(t.Context(), path, []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Errorf("got %v, want fs_write error-frame failure", err)
	}
}
