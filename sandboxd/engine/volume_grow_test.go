package engine

import (
	"encoding/json/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestGrowthQualifiedWireAndPrivateExpectation(t *testing.T) {
	// Captured from qualified Cocoon efe8897 v5 runtime evidence, without normalization.
	inspect, err := os.ReadFile("testdata/disk-inspect.json")
	if err != nil {
		t.Fatal(err)
	}
	grow, err := os.ReadFile("testdata/disk-grow.json")
	if err != nil {
		t.Fatal(err)
	}
	var expected DiskInspection
	var result DiskGrowResult
	if parseErr := json.Unmarshal(inspect, &expected, json.RejectUnknownMembers(true)); parseErr != nil {
		t.Fatal(parseErr)
	}
	if parseErr := json.Unmarshal(grow, &result, json.RejectUnknownMembers(true)); parseErr != nil {
		t.Fatal(parseErr)
	}
	if expected.Live.Binding.ObservedBytes != 20<<30 || result.Receipt.Request.TargetBytes != 40<<30 || !result.Receipt.Request.Expected.Equal(*expected.Live.Binding) || len(expected.Live.Binding.Backing.FDs) != 2 {
		t.Fatal("qualified schema changed")
	}
	dir := t.TempDir()
	t.Setenv("GROW_CAPTURE", dir)
	t.Setenv("GROW_INSPECT", string(inspect))
	t.Setenv("GROW_RESULT", string(grow))
	fakeCocoon(t, `#!/bin/sh
if [ "$1 $2 $3" = "vm disk inspect" ]; then printf '%s' "$GROW_INSPECT"; exit 0; fi
[ "$1 $2 $3 $4" = "vm disk grow --expect" ] || exit 72
cp "$5" "$GROW_CAPTURE/expected"
stat -c '%a' "$5" > "$GROW_CAPTURE/mode"
printf '%s' "$5" > "$GROW_CAPTURE/temp-path"
printf '%s\n' "$@" > "$GROW_CAPTURE/args"
printf '%s' "$GROW_RESULT"
echo "qualified runtime failure" >&2
exit 1
`)
	e := New("cocoon", nil, nil, false, false, "")
	got, err := e.InspectDisk(t.Context(), expected.Requested)
	if err != nil || !got.Live.Binding.Equal(*expected.Live.Binding) {
		t.Fatalf("inspection %+v %v", got, err)
	}
	receipt, err := e.GrowDisk(t.Context(), result.Receipt.Request)
	if err == nil || !strings.Contains(err.Error(), "cocoon vm disk:") || !strings.Contains(err.Error(), "qualified runtime failure") || receipt.Receipt == nil || receipt.Receipt.Outcome != "acknowledged" {
		t.Fatalf("nonzero stdout lost: %+v %v", receipt, err)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "expected"))
	var transported DiskInspection
	if err := json.Unmarshal(raw, &transported, json.RejectUnknownMembers(true)); err != nil || !transported.Live.Binding.Equal(*expected.Live.Binding) || transported.LatestReceipt != nil || transported.Error != nil {
		t.Fatalf("expected envelope: %s %v", raw, err)
	}
	mode, _ := os.ReadFile(filepath.Join(dir, "mode"))
	if strings.TrimSpace(string(mode)) != "600" {
		t.Fatalf("temp mode: %s", mode)
	}
	path, _ := os.ReadFile(filepath.Join(dir, "temp-path"))
	if _, err := os.Stat(string(path)); !os.IsNotExist(err) {
		t.Fatal("expect tempfile leaked")
	}
	args, _ := os.ReadFile(filepath.Join(dir, "args"))
	if !strings.Contains(string(args), "--target-bytes\n42949672960\n--output\njson\n") || strings.Contains(string(args), expected.Requested.VMID) {
		t.Fatalf("wrong grow args: %s", args)
	}
}

func TestGrowthGuestBlockWitnessDoesNotResizeFilesystem(t *testing.T) {
	sock := sockPath(t)
	fake := serveFakeSilkd(t, sock)
	configureVolumeDevices(fake)
	fake.mu.Lock()
	fake.serial["/sys/block/vdc/dev"] = []byte("8:32\n")
	fake.serial["/sys/block/vdc/size"] = []byte(strconv.FormatInt(40<<30/512, 10) + "\n")
	fake.serial["/proc/sys/kernel/random/boot_id"] = []byte("guest-boot\n")
	fake.serial["/proc/self/mountinfo"] = []byte(rootMountInfo + "27 1 8:32 / /data rw - ext4 /dev/vdc rw\n")
	fake.mu.Unlock()
	e := New("cocoon", nil, nil, false, false, "")
	got, err := e.ObserveVolume(t.Context(), sock, "imagenet", "/data")
	if err != nil || got.MountID != "27" || got.SizeBytes != 40<<30 || got.BootID != "guest-boot" || got.Device != "/dev/vdc" {
		t.Fatalf("guest witness %+v: %v", got, err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.execCalls) != 0 {
		t.Fatalf("observation executed guest tools: %v", fake.execCalls)
	}
}

func TestGrowthRejectsMalformedWireEvenWithAValidReceiptPrefix(t *testing.T) {
	raw, err := os.ReadFile("testdata/disk-grow.json")
	if err != nil {
		t.Fatal(err)
	}
	var qualified DiskGrowResult
	if parseErr := json.Unmarshal(raw, &qualified); parseErr != nil {
		t.Fatal(parseErr)
	}
	t.Setenv("GROW_BAD_JSON", strings.TrimSpace(string(raw))+" invalid trailing bytes")
	fakeCocoon(t, "#!/bin/sh\nprintf '%s' \"$GROW_BAD_JSON\"\nexit 1\n")
	e := New("cocoon", nil, nil, false, false, "")
	got, err := e.GrowDisk(t.Context(), qualified.Receipt.Request)
	if err == nil || got.Receipt != nil {
		t.Fatalf("partial malformed stdout accepted: %+v %v", got, err)
	}
}
