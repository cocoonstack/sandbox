package engine

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const balloonCocoon = `#!/bin/sh
echo "$*" >> "$COCOON_TEST_TRACE"
[ "$3" = --help ] && { printf 'Usage:\n  cocoon vm balloon <ref> <size> [flags]\n'; exit 0; }
case "$6" in
sbx-1) echo '{"vm":"sbx-1","target":2147483648,"actual":1073741824,"default":1073741824}' ;;
sbx-fc) echo '{"vm":"sbx-fc","target":2147483648,"default":1073741824}' ;;
sbx-zero) echo '{"vm":"sbx-zero","target":1073741824,"actual":0,"default":1073741824}' ;;
sbx-nodev) echo 'Error: vm sbx-nodev: balloon resize unsupported' >&2; exit 1 ;;
sbx-stopped) echo 'Error: vm sbx-stopped: vm not running' >&2; exit 1 ;;
sbx-big) echo 'Error: balloon size 4294967296 must be below guest memory 2147483648' >&2; exit 1 ;;
esac
`

func TestBalloonCallsBuildCocoonsArgsAndClassifyItsRefusals(t *testing.T) {
	trace := filepath.Join(t.TempDir(), "trace")
	t.Setenv("COCOON_TEST_TRACE", trace)
	fakeCocoon(t, balloonCocoon)
	e := New("cocoon", nil, nil, false, false, types.RestoreMmap)

	if err := e.CheckBalloonSupport(t.Context()); err != nil {
		t.Fatalf("support check: %v", err)
	}
	b, err := e.Balloon(t.Context(), "sbx-1", "50%")
	if err != nil || b.Target != 2<<30 || b.Actual == nil || *b.Actual != 1<<30 || b.Default != 1<<30 {
		t.Fatalf("balloon = %+v, %v", b, err)
	}
	if b, err = e.Balloon(t.Context(), "sbx-fc", "1073741824"); err != nil || b.Actual != nil || b.Default != 1<<30 {
		t.Fatalf("balloon without actual = %+v, %v", b, err)
	}
	if b, err = e.Balloon(t.Context(), "sbx-zero", "1073741824"); err != nil || b.Actual == nil || *b.Actual != 0 {
		t.Fatalf("balloon holding nothing = %+v, %v, want a reported zero", b, err)
	}
	if _, err = e.Balloon(t.Context(), "sbx-nodev", "50%"); !errors.Is(err, ErrBalloonUnsupported) {
		t.Errorf("no balloon device: %v, want ErrBalloonUnsupported", err)
	}
	if _, err = e.Balloon(t.Context(), "sbx-stopped", "50%"); err == nil || errors.Is(err, ErrBalloonUnsupported) || errors.Is(err, ErrBalloonTarget) || !strings.Contains(err.Error(), "vm not running") {
		t.Errorf("stopped vm: %v, want an error carrying cocoon's reason", err)
	}
	if _, err = e.Balloon(t.Context(), "sbx-big", "4G"); !errors.Is(err, ErrBalloonTarget) || !strings.Contains(err.Error(), "below guest memory") {
		t.Errorf("target past guest memory: %v, want ErrBalloonTarget carrying cocoon's reason", err)
	}

	data, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"vm balloon --help",
		"vm balloon --output json -- sbx-1 50%",
		"vm balloon --output json -- sbx-fc 1073741824",
		"vm balloon --output json -- sbx-zero 1073741824",
		"vm balloon --output json -- sbx-nodev 50%",
		"vm balloon --output json -- sbx-stopped 50%",
		"vm balloon --output json -- sbx-big 4G",
	}
	if calls := strings.Split(strings.TrimSpace(string(data)), "\n"); !slices.Equal(calls, want) {
		t.Errorf("cocoon calls\n%q\nwant\n%q", calls, want)
	}
}

func TestBalloonSupportRejectsACocoonThatPrintsTheVMHelp(t *testing.T) {
	fakeCocoon(t, "#!/bin/sh\nprintf 'Usage:\\n  cocoon vm [command]\\n'\n")
	e := New("cocoon", nil, nil, false, false, types.RestoreMmap)
	if err := e.CheckBalloonSupport(t.Context()); err == nil {
		t.Error("a cocoon without vm balloon passed the support check")
	}
}
