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
case "$3" in
--help) printf 'Usage:\n  cocoon vm balloon <ref> <size> [flags]\n' ;;
sbx-1) echo '{"vm":"sbx-1","target":2147483648,"actual":1073741824,"default":1073741824}' ;;
sbx-fc) echo '{"vm":"sbx-fc","target":2147483648,"default":1073741824}' ;;
sbx-nodev) echo 'Error: vm sbx-nodev: balloon resize unsupported' >&2; exit 1 ;;
sbx-stopped) echo 'Error: vm sbx-stopped: vm not running' >&2; exit 1 ;;
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
	if err != nil || b != (types.VMBalloon{Target: 2 << 30, Actual: 1 << 30, Default: 1 << 30}) {
		t.Fatalf("balloon = %+v, %v", b, err)
	}
	if b, err = e.Balloon(t.Context(), "sbx-fc", "1073741824"); err != nil || b.Actual != 0 || b.Default != 1<<30 {
		t.Fatalf("balloon without actual = %+v, %v", b, err)
	}
	if _, err = e.Balloon(t.Context(), "sbx-nodev", "50%"); !errors.Is(err, ErrBalloonUnsupported) {
		t.Errorf("no balloon device: %v, want ErrBalloonUnsupported", err)
	}
	if _, err = e.Balloon(t.Context(), "sbx-stopped", "50%"); err == nil || errors.Is(err, ErrBalloonUnsupported) || !strings.Contains(err.Error(), "vm not running") {
		t.Errorf("stopped vm: %v, want a retryable error carrying cocoon's reason", err)
	}

	data, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"vm balloon --help",
		"vm balloon sbx-1 50% --output json",
		"vm balloon sbx-fc 1073741824 --output json",
		"vm balloon sbx-nodev 50% --output json",
		"vm balloon sbx-stopped 50% --output json",
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
