package engine

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const migrateCocoon = `#!/bin/sh
echo "$*" >> "$COCOON_TEST_TRACE"
case "$3" in
capture) echo '{"series":"s1","dir":"/run/xfer/s1","round":2,"paused":true,"files":[{"name":"state.json","size":3},{"name":"cow.raw","size":4096}]}' ;;
receive) echo '{"state":"created","vsock_socket":"/run/sbx-1/vsock.sock","config":{"name":"sbx-1"}}' ;;
esac
`

func TestMigrateCallsBuildCocoonsArgsAndParseItsJSON(t *testing.T) {
	trace := filepath.Join(t.TempDir(), "trace")
	t.Setenv("COCOON_TEST_TRACE", trace)
	fakeCocoon(t, migrateCocoon)
	e := New("cocoon", []string{"br0"}, nil, false, false, types.RestoreMmap)
	none := types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall}
	egress := types.PoolKey{Template: "rt:24.04", Net: types.NetEgress, Size: types.SizeSmall}

	if _, err := e.MigrateCapture(t.Context(), "sbx-1", "", false); err != nil {
		t.Fatalf("baseline capture: %v", err)
	}
	round, err := e.MigrateCapture(t.Context(), "sbx-1", "s1", true)
	if err != nil {
		t.Fatalf("final capture: %v", err)
	}
	want := types.MigrateRound{Series: "s1", Dir: "/run/xfer/s1", Round: 2, Paused: true, Files: []types.MigrateFile{{Name: "state.json", Size: 3}, {Name: "cow.raw", Size: 4096}}}
	if !reflect.DeepEqual(round, want) {
		t.Errorf("round = %+v, want %+v", round, want)
	}
	for _, key := range []types.PoolKey{none, egress} {
		rec, receiveErr := e.MigrateReceive(t.Context(), "/xfer/s1", "sbx-1", key)
		if receiveErr != nil || rec.VsockSocket != "/run/sbx-1/vsock.sock" || rec.Config.Name != "sbx-1" {
			t.Fatalf("receive on %s = %+v, %v", key.Net, rec, receiveErr)
		}
	}
	if err = e.MigrateAbort(t.Context(), "sbx-1", "s1"); err != nil {
		t.Fatalf("abort: %v", err)
	}

	data, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	receive := "vm migrate receive --from-dir /xfer/s1 --name sbx-1 --pull --output json --no-direct-io=false --restore-mode mmap"
	wantCalls := []string{
		"vm migrate capture sbx-1 --output json",
		"vm migrate capture sbx-1 --series s1 --final --output json",
		receive,
		receive + " --bridge br0",
		"vm migrate abort --series s1 sbx-1",
	}
	if calls := strings.Split(strings.TrimSpace(string(data)), "\n"); !slices.Equal(calls, wantCalls) {
		t.Errorf("cocoon calls\n%q\nwant\n%q", calls, wantCalls)
	}
}

func TestMigrateCallsSurfaceCocoonsStderr(t *testing.T) {
	fakeCocoon(t, "#!/bin/sh\necho 'Error: capture refused: run dir has no reflink' >&2\nexit 1\n")
	e := newTestEngine()
	_, captureErr := e.MigrateCapture(t.Context(), "sbx-1", "", false)
	_, receiveErr := e.MigrateReceive(t.Context(), "/xfer/s1", "sbx-1", types.PoolKey{Net: types.NetNone})
	abortErr := e.MigrateAbort(t.Context(), "sbx-1", "s1")
	for name, err := range map[string]error{"capture": captureErr, "receive": receiveErr, "abort": abortErr} {
		if err == nil || !strings.Contains(err.Error(), "cocoon vm migrate") || !strings.Contains(err.Error(), "run dir has no reflink") {
			t.Errorf("%s error = %v, want cocoon's stderr tail", name, err)
		}
	}
}

func TestMigrateCaptureRejectsOutputThatIsNotARound(t *testing.T) {
	fakeCocoon(t, "#!/bin/sh\necho 'series s1 round 0'\n")
	if _, err := newTestEngine().MigrateCapture(t.Context(), "sbx-1", "", false); err == nil || !strings.Contains(err.Error(), "parse migrate capture") {
		t.Errorf("capture of prose = %v, want a parse error", err)
	}
}

func TestMigrateReceiveLeavesTheOndemandRestoreModeToCocoon(t *testing.T) {
	trace := filepath.Join(t.TempDir(), "trace")
	t.Setenv("COCOON_TEST_TRACE", trace)
	fakeCocoon(t, migrateCocoon)
	key := types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall}
	for _, mode := range []types.RestoreMode{types.RestoreOnDemand, types.RestoreCopy} {
		e := New("cocoon", nil, nil, false, false, mode)
		if _, err := e.MigrateReceive(t.Context(), "/xfer/s1", "sbx-1", key); err != nil {
			t.Fatalf("receive under %s: %v", mode, err)
		}
		_, _ = e.Clone(t.Context(), "/golden", "sbx-2", key)
	}
	data, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	calls := strings.Split(strings.TrimSpace(string(data)), "\n")
	receive := "vm migrate receive --from-dir /xfer/s1 --name sbx-1 --pull --output json --no-direct-io=false"
	clone := "vm clone --from-dir /golden --name sbx-2 --pull --output json --no-direct-io=false --restore-mode "
	want := []string{receive, clone + "ondemand", receive + " --restore-mode copy", clone + "copy"}
	if !slices.Equal(calls, want) {
		t.Errorf("cocoon calls\n%q\nwant\n%q", calls, want)
	}
}
