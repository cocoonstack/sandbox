package engine

import (
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

func TestStartReaderAssemblesTheViewTopLayerFirst(t *testing.T) {
	sock := sockPath(t)
	fake := serveFakeSilkd(t, sock)
	configureReaderDevices(fake)
	e := New("cocoon", nil, nil, false, false, "")

	if err := e.StartReader(t.Context(), sock, ReaderDisks{Layers: []string{"rl0", "rl1"}, Cow: "rcow"}); err != nil {
		t.Fatalf("StartReader: %v", err)
	}
	want := [][]string{
		{"mkdir", "-p", "--", "/run/reader/cow"},
		{"mount", "-o", "rw", "--", "/dev/vdb", "/run/reader/cow"},
		{"mkdir", "-p", "--", "/run/reader/layer1"},
		{"mount", "-o", "ro", "--", "/dev/vdd", "/run/reader/layer1"},
		{"mkdir", "-p", "--", "/run/reader/layer0"},
		{"mount", "-o", "ro", "--", "/dev/vdc", "/run/reader/layer0"},
		{"mkdir", "-p", "--", "/view"},
		{"mount", "-t", "overlay", "-o", "ro,metacopy=on,redirect_dir=follow,lowerdir=/run/reader/cow/upper:/run/reader/layer1:/run/reader/layer0", "--", "overlay", "/view"},
		{"env", "SILKD_PORT=2051", "chroot", "/view", "/usr/local/bin/silkd"},
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.execCalls) != len(want) {
		t.Fatalf("guest ran %d commands %v, want %d", len(fake.execCalls), fake.execCalls, len(want))
	}
	for i, argv := range want {
		if !slices.Equal(fake.execCalls[i], argv) {
			t.Errorf("command %d = %v, want %v", i, fake.execCalls[i], argv)
		}
	}
	if last := fake.connects[len(fake.connects)-1]; last != "CONNECT 2051" {
		t.Errorf("readiness probe dialed %q, want the view silkd's port", last)
	}
	for i, detached := range fake.execDetach {
		if detached != (i == len(want)-1) {
			t.Errorf("command %d detached=%v, want only the view silkd detached", i, detached)
		}
	}
}

func TestStartReaderStopsAtTheFirstFailedMount(t *testing.T) {
	sock := sockPath(t)
	fake := serveFakeSilkd(t, sock)
	configureReaderDevices(fake)
	fake.mu.Lock()
	fake.execFailAt, fake.execCode = 4, 32
	fake.mu.Unlock()

	err := New("cocoon", nil, nil, false, false, "").StartReader(t.Context(), sock, ReaderDisks{Layers: []string{"rl0", "rl1"}, Cow: "rcow"})
	if err == nil || !strings.Contains(err.Error(), "mount layer 1") {
		t.Fatalf("StartReader = %v, want the layer 1 mount failure", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.execCalls) != 4 {
		t.Errorf("guest ran %d commands after the failed mount, want it to stop at 4: %v", len(fake.execCalls), fake.execCalls)
	}
}

func TestDialReaderConnectsToTheViewPort(t *testing.T) {
	sock := sockPath(t)
	fake := serveFakeSilkd(t, sock)
	conn, err := New("cocoon", nil, nil, false, false, "").DialReader(t.Context(), sock)
	if err != nil {
		t.Fatalf("DialReader: %v", err)
	}
	_ = conn.Close()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if !slices.Equal(fake.connects, []string{"CONNECT 2051"}) {
		t.Errorf("connects = %v, want one CONNECT 2051", fake.connects)
	}
}

func configureReaderDevices(f *fakeSilkd) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.block = []wire.DirEntry{
		{Name: "vda", Kind: wire.FileKindSymlink},
		{Name: "vdb", Kind: wire.FileKindSymlink},
		{Name: "vdc", Kind: wire.FileKindSymlink},
		{Name: "vdd", Kind: wire.FileKindSymlink},
	}
	for dev, serial := range map[string]string{"vda": "root", "vdb": "rcow", "vdc": "rl0", "vdd": "rl1"} {
		f.serial["/sys/block/"+dev+"/serial"] = []byte(serial + "\n")
		f.stat["/dev/"+dev] = true
	}
}
