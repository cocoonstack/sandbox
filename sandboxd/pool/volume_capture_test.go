package pool

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestVolumeClaimRejectsDirectCapture(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	sb.Volumes = []types.Volume{{Name: "dataset", Mount: "/datasets"}}

	tests := []struct {
		name string
		run  func(*testing.T) error
	}{
		{"direct hibernate", func(t *testing.T) error { return m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}) }},
		{"fork", func(t *testing.T) error {
			_, err := m.Fork(t.Context(), sb.ID, Cred{Token: sb.Token}, 1, time.Hour)
			return err
		}},
		{"checkpoint", func(t *testing.T) error {
			_, err := m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "", "")
			return err
		}},
		{"promote", func(t *testing.T) error {
			_, _, err := m.Promote(t.Context(), sb.ID, Cred{Token: sb.Token}, "volume-source", "")
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(t); !errors.Is(err, ErrVolumeCapture) {
				t.Errorf("error %v, want ErrVolumeCapture", err)
			}
		})
	}
	if len(eng.hibernates) != 0 || len(eng.snapSaves) != 0 || len(eng.exports) != 0 {
		t.Errorf("capture reached engine: hibernates=%v snapshots=%v exports=%v", eng.hibernates, eng.snapSaves, eng.exports)
	}
}

func TestVolumeClaimRunningWakeUnchanged(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	sb.Volumes = []types.Volume{{Name: "dataset", Mount: "/datasets"}}

	sock, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if err != nil {
		t.Fatalf("WakeAgentSocket: %v", err)
	}
	if sock != sb.VsockSocket {
		t.Errorf("socket %q, want %q", sock, sb.VsockSocket)
	}
	if len(eng.restores) != 0 {
		t.Errorf("running wake restored %v, want no restore", eng.restores)
	}
}

func TestReconcileRetainsVolumeCaptureGateWithoutCatalog(t *testing.T) {
	eng := newFakeEngine()
	dataDir := t.TempDir()
	volumePath := writeVolumeImage(t, "dataset.img", "data")
	m, err := NewManager(t.Context(), &config.Config{
		DataDir: dataDir,
		Volumes: []config.VolumeSpec{{Name: "dataset", Path: volumePath, DirectIO: "off"}},
	}, eng, testSecrets(t))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	sb, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "dataset", Mount: "/datasets"}})
	if err != nil {
		t.Fatalf("ClaimProvision: %v", err)
	}

	// Restart from the persisted claim after removing the catalog entry.
	m2 := newTestManagerAt(t, eng, dataDir)
	if err := m2.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	for _, tt := range []struct {
		name string
		run  func() error
	}{
		{"after restart hibernate", func() error { return m2.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}) }},
		{"after restart fork", func() error {
			_, forkErr := m2.Fork(t.Context(), sb.ID, Cred{Token: sb.Token}, 1, time.Hour)
			return forkErr
		}},
		{"after restart checkpoint", func() error {
			_, checkpointErr := m2.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "", "")
			return checkpointErr
		}},
		{"after restart promote", func() error {
			_, _, promoteErr := m2.Promote(t.Context(), sb.ID, Cred{Token: sb.Token}, "volume-source", "")
			return promoteErr
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if captureErr := tt.run(); !errors.Is(captureErr, ErrVolumeCapture) {
				t.Errorf("error %v, want ErrVolumeCapture", captureErr)
			}
		})
	}
	if got, ok := m2.claimed[sb.ID]; !ok || !slices.Equal(got.Volumes, []types.Volume{{Name: "dataset", Mount: "/datasets"}}) {
		t.Errorf("reconciled claim=%+v, want persisted volume", got)
	}
	if len(eng.hibernates) != 0 || len(eng.snapSaves) != 0 || len(eng.exports) != 0 {
		t.Errorf("capture reached engine: hibernates=%v snapshots=%v exports=%v", eng.hibernates, eng.snapSaves, eng.exports)
	}
}

func TestIdleOnceSkipsVolumeClaim(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, IdleHibernateSeconds: 1})
	sb := mustClaim(t, m, testKey)
	sb.Volumes = []types.Volume{{Name: "dataset", Mount: "/datasets"}}
	backdate(m, sb, 2*time.Second)

	m.idleOnce(t.Context())
	waitFor(t, func() bool { return !m.idleSweep.Load() })
	if len(eng.hibernates) != 0 || hibernated(m) != 0 {
		t.Errorf("idle sweep hibernated volume claim: engine=%v count=%d", eng.hibernates, hibernated(m))
	}
}
