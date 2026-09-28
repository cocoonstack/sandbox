package pool

import (
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestSetPoolsRejectsCaptureTrim(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1})
	err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: testKey, Warm: 1, CaptureTrim: true}})
	if !errors.Is(err, ErrBadKey) || !strings.Contains(err.Error(), "capture_trim is set in the config file") {
		t.Errorf("SetPools error = %v, want ErrBadKey naming capture_trim as config-owned", err)
	}
}

func TestCaptureTrimRunsBeforeTheSnapOnlyForItsPool(t *testing.T) {
	eng := newFakeEngine()
	other := types.PoolKey{Template: "py:3.12", Net: testKey.Net, Size: testKey.Size}
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, CaptureTrim: true}, config.PoolSpec{PoolKey: other, Warm: 1})

	trimmed := mustClaim(t, m, testKey)
	key, _, err := m.Promote(t.Context(), trimmed.ID, Cred{Token: trimmed.Token}, "tpl:trim", "")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if _, err = m.Checkpoint(t.Context(), trimmed.ID, Cred{Token: trimmed.Token}, "c1", ""); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	clone, err := m.ClaimProvisionPromoted(t.Context(), key, ClaimOptions{})
	if err != nil {
		t.Fatalf("claim the template: %v", err)
	}
	if _, err = m.Checkpoint(t.Context(), clone.ID, Cred{Token: clone.Token}, "c2", ""); err != nil {
		t.Fatalf("checkpoint the clone: %v", err)
	}
	untouched := mustClaim(t, m, other)
	if _, _, err = m.Promote(t.Context(), untouched.ID, Cred{Token: untouched.Token}, "tpl:plain", ""); err != nil {
		t.Fatalf("promote the untrimmed pool: %v", err)
	}
	if _, err = m.Checkpoint(t.Context(), untouched.ID, Cred{Token: untouched.Token}, "c3", ""); err != nil {
		t.Fatalf("checkpoint the untrimmed pool: %v", err)
	}

	want := []string{filepath.Base(trimmed.VsockSocket) + " after 0 snaps", filepath.Base(trimmed.VsockSocket) + " after 1 snaps", filepath.Base(clone.VsockSocket) + " after 2 snaps"}
	if !slices.Equal(eng.trims, want) {
		t.Errorf("trims %v, want %v: before each capture of the trimmed pool and its template's clone, never the other pool", eng.trims, want)
	}
}

func TestAFailedTrimStillCaptures(t *testing.T) {
	eng := newFakeEngine()
	eng.trimErr = errors.New("fstrim: operation not supported")
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, CaptureTrim: true})
	sb := mustClaim(t, m, testKey)
	if _, _, err := m.Promote(t.Context(), sb.ID, Cred{Token: sb.Token}, "tpl:trimfail", ""); err != nil {
		t.Errorf("promote after a failed trim: %v", err)
	}
	if _, err := m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "c1", ""); err != nil {
		t.Errorf("checkpoint after a failed trim: %v", err)
	}
	if len(eng.trims) != 2 || len(eng.snapSaves) != 2 {
		t.Errorf("trims %v snaps %v, want both captures to have tried the trim and captured anyway", eng.trims, eng.snapSaves)
	}
}

func TestCaptureTrimSkipsASandboxThatIsNotRunning(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, CaptureTrim: true})
	m.trimForCapture(t.Context(), &types.Sandbox{ID: "sb_h", Key: testKey, HibernateSnap: "snap"})
	m.trimForCapture(t.Context(), &types.Sandbox{ID: "sb_a", Key: testKey, ArchiveCk: "ck_1"})
	if len(eng.trims) != 0 {
		t.Errorf("trims %v, want none for a hibernated or archived sandbox", eng.trims)
	}
}
