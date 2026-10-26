package engine

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestVMEventsDeliversTheSnapshotThenChanges(t *testing.T) {
	socket := sockPath(t)
	serveCocoond(t, socket, "event: sync\ndata: {\"vms\":[{\"backend\":\"cloud-hypervisor\",\"id\":\"X\",\"name\":\"sbx-1\",\"state\":\"running\",\"live\":true}]}\n\n"+
		"event: change\ndata: {\"type\":\"MODIFIED\",\"vm\":{\"name\":\"sbx-1\",\"state\":\"stopped\",\"live\":false,\"last_transition_reason\":\"unexpected-exit\"}}\n\n")
	var synced []VMStatus
	var changes []VMChange
	err := New("cocoon", nil, nil, false, false, "").VMEvents(t.Context(), socket,
		func(vms []VMStatus) { synced = vms },
		func(c VMChange) { changes = append(changes, c) })
	if err == nil {
		t.Fatal("VMEvents returned nil after the daemon closed the stream")
	}
	if len(synced) != 1 || synced[0].Name != "sbx-1" || !synced[0].Live {
		t.Errorf("sync = %+v, want sbx-1 live", synced)
	}
	if len(changes) != 1 || changes[0].Kind != "MODIFIED" || changes[0].VM.Name != "sbx-1" || changes[0].VM.Live {
		t.Errorf("changes = %+v, want one unexpected exit of sbx-1", changes)
	}
}

func TestVMEventsFailsWithoutADaemon(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err := New("cocoon", nil, nil, false, false, "").VMEvents(ctx, sockPath(t), func([]VMStatus) {}, func(VMChange) {})
	if err == nil {
		t.Fatal("VMEvents succeeded with no socket")
	}
}

func TestInspectReadsOneVMAndReportsAMissingOne(t *testing.T) {
	fakeCocoon(t, "#!/bin/sh\n[ \"$1 $2\" = \"vm inspect\" ] || exit 2\n"+
		"if [ \"$3\" = sbx-1 ]; then echo '{\"config\":{\"name\":\"sbx-1\"},\"state\":\"running\","+
		"\"attached_devices\":{\"disks\":[{\"id\":\"disk-a\",\"name\":\"imagenet\",\"path\":\"/v/a.img\",\"readonly\":true},{\"id\":\"disk-b\",\"name\":\"scratch\",\"path\":\"/v/b.img\"}]}}'; exit 0; fi\n"+
		"if [ \"$3\" = sbx-stale ]; then echo '{\"state\":\"stopped\",\"stale\":true}'; exit 0; fi\n"+
		"if [ \"$3\" = sbx-stopped ]; then echo '{\"state\":\"stopped\"}'; exit 0; fi\n"+
		"echo \"Error: inspect: vm $3: vm not found\" >&2; exit 1\n")
	e := New("cocoon", nil, nil, false, false, "")
	rec, ok, err := e.Inspect(t.Context(), "sbx-1")
	wantDisks := []types.VMDisk{{Name: "imagenet", ReadOnly: true}, {Name: "scratch"}}
	if err != nil || !ok || rec.State != "running" || rec.Config.Name != "sbx-1" || !slices.Equal(rec.AttachedDevices.Disks, wantDisks) {
		t.Errorf("Inspect sbx-1 = %+v, %v, %v", rec, ok, err)
	}
	for _, name := range []string{"sbx-stale", "sbx-stopped"} {
		rec, present, err := e.Inspect(t.Context(), name)
		if err != nil || !present || rec.State != "stopped" || rec.Stale != (name == "sbx-stale") {
			t.Errorf("Inspect %s lost stopped classification: %+v, %v, %v", name, rec, present, err)
		}
	}
	if _, ok, err := e.Inspect(t.Context(), "sbx-gone"); err != nil || ok {
		t.Errorf("Inspect of a missing VM = %v, %v; want not found without an error", ok, err)
	}
}

func TestInspectPreservesMissingDiskInventory(t *testing.T) {
	for _, tc := range []struct {
		name, inventory string
		unknown         bool
	}{
		{"omitted devices", "", true},
		{"omitted disks", `,"attached_devices":{}`, true},
		{"null disks", `,"attached_devices":{"disks":null}`, true},
		{"empty disks", `,"attached_devices":{"disks":[]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeCocoon(t, "#!/bin/sh\necho '{\"state\":\"running\""+tc.inventory+"}'\n")
			rec, present, err := New("cocoon", nil, nil, false, false, "").Inspect(t.Context(), "sbx-1")
			if err != nil || !present || (rec.AttachedDevices.Disks == nil) != tc.unknown {
				t.Fatalf("inventory classification: %+v, %v, %v", rec, present, err)
			}
		})
	}
}

func serveCocoond(t *testing.T, socket, stream string) {
	t.Helper()
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/events" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, stream)
	}), ReadHeaderTimeout: time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
}
