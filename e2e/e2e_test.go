// Package e2e drives the full host stack in one process: real pool manager,
// real engine dialing real hybrid-vsock sockets, real HTTP server and relay,
// real SDK — only cocoon and the guest are faked. It doubles as the drift
// guard between the SDK's wire mirrors and sandboxd's types.
package e2e

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/server"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

var testKey = types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall, Engine: types.EngineCH}

func TestEndToEnd(t *testing.T) {
	stack := startStack(t, "node-token", config.PoolSpec{PoolKey: testKey, Warm: 1})

	var sb *sandbox.Sandbox
	t.Run("cold claim before any refill", func(t *testing.T) {
		var err error
		sb, err = stack.client.New(t.Context(), "rt:24.04")
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		out, err := sb.Exec(t.Context(), "echo", "42")
		if err != nil {
			t.Fatalf("Exec: %v", err)
		}
		if out != "42\n" {
			t.Errorf("stdout %q, want 42\\n", out)
		}
	})

	t.Run("refill then warm claim", func(t *testing.T) {
		// Warm-hit = zero VM ops is proven by the pool unit tests; asserting
		// create counts here would race the background refill ticker.
		waitFor(t, func() bool {
			infos, _ := stack.mgr.Info()
			return len(infos) == 1 && infos[0].Warm >= 1
		})
		warm, err := stack.client.New(t.Context(), "rt:24.04")
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer warm.Close()
		if out, err := warm.Exec(t.Context(), "echo", "warm"); err != nil || out != "warm\n" {
			t.Errorf("exec on warm claim: %q, %v", out, err)
		}
	})

	t.Run("close releases and revokes access", func(t *testing.T) {
		if sb == nil {
			t.Skip("cold claim subtest failed")
		}
		if err := sb.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, err := sb.Exec(t.Context(), "echo", "zombie"); err == nil ||
			!strings.Contains(err.Error(), "unknown sandbox") {
			t.Errorf("got %v, want unknown sandbox after release", err)
		}
		if err := sb.Close(); err != nil {
			t.Errorf("second Close: %v", err)
		}
	})
}

func TestForkEndToEnd(t *testing.T) {
	stack := startStack(t, "node-token")
	parent, err := stack.client.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer parent.Close()

	children, err := parent.Fork(t.Context(), 2, time.Minute)
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if len(children) != 2 {
		t.Fatalf("got %d children, want 2", len(children))
	}
	for i, child := range children {
		if out, err := child.Exec(t.Context(), "echo", "kid"); err != nil || out != "kid\n" {
			t.Fatalf("child %d exec: %q, %v", i, out, err)
		}
	}
	// Children are independent claims: releasing one leaves the sibling and
	// the parent alive.
	if err := children[0].Close(); err != nil {
		t.Fatalf("close child 0: %v", err)
	}
	if _, err := children[1].Exec(t.Context(), "echo", "alive"); err != nil {
		t.Errorf("sibling died with the released child: %v", err)
	}
	if _, err := parent.Exec(t.Context(), "echo", "alive"); err != nil {
		t.Errorf("parent died with the released child: %v", err)
	}
	_ = children[1].Close()
}

func TestPromoteEndToEnd(t *testing.T) {
	stack := startStack(t, "node-token")
	parent, err := stack.client.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer parent.Close()

	tpl, err := parent.Promote(t.Context(), "e2e-tpl:1")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if tpl.ContentDigest == "" {
		t.Fatal("Promote returned an empty content digest")
	}
	// Both claim surfaces must work: the owner-bound handle and the
	// name-based Client call on the (single) node that holds the template.
	child, err := tpl.New(t.Context())
	if err != nil {
		t.Fatalf("claim via template handle: %v", err)
	}
	if child.TemplateDigest != tpl.ContentDigest {
		t.Errorf("claim template digest %q, want %q", child.TemplateDigest, tpl.ContentDigest)
	}
	if out, err := child.Exec(t.Context(), "echo", "tpl"); err != nil || out != "tpl\n" {
		t.Errorf("exec on promoted claim: %q, %v", out, err)
	}
	_ = child.Close()
	byName, nameErr := stack.client.New(t.Context(), "e2e-tpl:1")
	if nameErr != nil {
		t.Fatalf("claim promoted template by name: %v", nameErr)
	}
	if byName.TemplateDigest != tpl.ContentDigest {
		t.Errorf("name claim template digest %q, want %q", byName.TemplateDigest, tpl.ContentDigest)
	}
	_ = byName.Close()

	if err := tpl.Delete(t.Context()); err != nil {
		t.Fatalf("Template.Delete: %v", err)
	}
	if err := stack.client.DeleteTemplate(t.Context(), "e2e-tpl:1"); err == nil ||
		!strings.Contains(err.Error(), "unknown template") {
		t.Errorf("second delete: %v, want unknown template", err)
	}
}

func TestCheckpointEndToEnd(t *testing.T) {
	stack := startStack(t, "node-token")
	src, err := stack.client.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer src.Close()

	ckpt, err := src.Checkpoint(t.Context(), "step-1")
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if ckpt.SandboxID != src.ID || ckpt.Name != "step-1" {
		t.Errorf("record %+v, want bound to %s", ckpt, src.ID)
	}
	if out, execErr := src.Exec(t.Context(), "echo", "still-alive"); execErr != nil || out != "still-alive\n" {
		t.Fatalf("source after checkpoint: %q, %v", out, execErr)
	}

	branch, err := ckpt.New(t.Context())
	if err != nil {
		t.Fatalf("branch: %v", err)
	}
	if branch.ID == src.ID {
		t.Error("branch reused the source id")
	}
	if out, execErr := branch.Exec(t.Context(), "echo", "branched"); execErr != nil || out != "branched\n" {
		t.Errorf("exec on branch: %q, %v", out, execErr)
	}
	_ = branch.Close()

	ckpts, err := stack.client.Checkpoints(t.Context())
	if err != nil || len(ckpts) != 1 || ckpts[0].ID != ckpt.ID {
		t.Fatalf("Checkpoints() = %+v, %v; want the one record", ckpts, err)
	}
	if err := ckpts[0].Delete(t.Context()); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := ckpt.New(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "unknown checkpoint") {
		t.Errorf("branch after delete: %v, want unknown checkpoint", err)
	}
}

// TestTwoTenantFlow drives two tenants through the real stack: each claims
// under its own token, quotas bind per tenant, checkpoint listings are
// isolated, and operator surfaces refuse tenant tokens with 403.
func TestTwoTenantFlow(t *testing.T) {
	tenants := []config.TenantSpec{
		{Name: "acme", Token: "acme-tok", MaxClaims: 1},
		{Name: "beta", Token: "beta-tok"},
	}
	stack := startTenantStack(t, "node-token", tenants, nil)
	acme, err := sandbox.Connect(stack.addr, sandbox.WithAPIToken("acme-tok"))
	if err != nil {
		t.Fatalf("connect acme: %v", err)
	}
	beta, err := sandbox.Connect(stack.addr, sandbox.WithAPIToken("beta-tok"))
	if err != nil {
		t.Fatalf("connect beta: %v", err)
	}

	sbA, err := acme.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("acme claim: %v", err)
	}
	defer sbA.Close()
	if _, capErr := acme.New(t.Context(), "rt:24.04"); capErr == nil ||
		!strings.Contains(capErr.Error(), "429") {
		t.Errorf("acme second claim past its cap: %v, want 429", capErr)
	}
	sbB, err := beta.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("beta claim while acme is at cap: %v", err)
	}
	defer sbB.Close()

	if _, err := sbA.Checkpoint(t.Context(), "acme-step"); err != nil {
		t.Fatalf("acme checkpoint: %v", err)
	}
	if _, err := sbB.Checkpoint(t.Context(), "beta-step"); err != nil {
		t.Fatalf("beta checkpoint: %v", err)
	}
	for _, tt := range []struct {
		client *sandbox.Client
		want   []string
	}{
		{acme, []string{"acme-step"}},
		{beta, []string{"beta-step"}},
		{stack.client, []string{"acme-step", "beta-step"}},
	} {
		ckpts, err := tt.client.Checkpoints(t.Context())
		if err != nil {
			t.Fatalf("list checkpoints: %v", err)
		}
		var names []string
		for _, ck := range ckpts {
			names = append(names, ck.Name)
		}
		slices.Sort(names)
		if !slices.Equal(names, tt.want) {
			t.Errorf("checkpoint listing %v, want %v", names, tt.want)
		}
	}

	if _, err := acme.Info(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "403") {
		t.Errorf("tenant on operator surface: %v, want 403", err)
	}
	if _, err := stack.client.Info(t.Context()); err != nil {
		t.Errorf("root on operator surface: %v", err)
	}
}

func TestWrongAPITokenRejected(t *testing.T) {
	stack := startStack(t, "node-token")

	bad, err := sandbox.Connect(stack.addr, sandbox.WithAPIToken("wrong"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := bad.New(t.Context(), "rt:24.04"); err == nil ||
		!strings.Contains(err.Error(), "401") {
		t.Errorf("got %v, want 401", err)
	}
}

func TestVolumesEndToEnd(t *testing.T) {
	image := writeVolumeImage(t, "dataset.img", "dataset-bytes")
	scratch := writeVolumeImage(t, "scratch.img", "scratch-bytes")
	stack := startTenantStack(t, "node-token", nil,
		[]config.VolumeSpec{
			{Name: "dataset", Path: image, DirectIO: "off"},
			{Name: "scratch", Path: scratch, Writable: true},
		},
		config.PoolSpec{PoolKey: testKey, Warm: 1})
	waitFor(t, func() bool {
		infos, _ := stack.mgr.Info()
		return len(infos) == 1 && infos[0].Warm >= 1
	})
	warmBefore := stack.mgr.Counters().ClaimsWarm

	sb, err := stack.client.New(t.Context(), "rt:24.04",
		sandbox.WithVolumes(sandbox.Volume{Name: "dataset", Mount: "/datasets/e2e"}))
	if err != nil {
		t.Fatalf("volume claim: %v", err)
	}
	defer sb.Close()
	if want := []sandbox.Volume{{Name: "dataset", Mount: "/datasets/e2e"}}; !slices.Equal(sb.Volumes, want) {
		t.Errorf("claim volumes %+v, want %+v", sb.Volumes, want)
	}
	if counters := stack.mgr.Counters(); counters.ClaimsWarm != warmBefore+1 {
		infos, _ := stack.mgr.Info()
		t.Errorf("counters=%+v pools=%+v, want warm claims %d", counters, infos, warmBefore+1)
	}

	infos, err := stack.client.Volumes(t.Context())
	if err != nil {
		t.Fatalf("Volumes: %v", err)
	}
	want := []sandbox.VolumeInfo{{
		Name: "dataset", DefaultMount: "/volumes/dataset",
		SizeBytes: int64(len("dataset-bytes")), Available: true, Nodes: 1,
	}, {
		Name: "scratch", DefaultMount: "/volumes/scratch",
		SizeBytes: int64(len("scratch-bytes")), Available: true, Nodes: 1, Writable: true,
	}}
	if !slices.Equal(infos, want) {
		t.Errorf("catalog %+v, want %+v", infos, want)
	}

	var listed struct {
		Volumes []map[string]any `json:"volumes"`
	}
	_, body := rawJSON(t, stack, http.MethodGet, "/v1/volumes", "")
	if err := json.Unmarshal(body, &listed); err != nil {
		t.Fatalf("decode catalog %s: %v", body, err)
	}
	if len(listed.Volumes) != len(want) {
		t.Fatalf("catalog bytes %s, want %d entries", body, len(want))
	}
	for _, entry := range listed.Volumes {
		var writable any
		if entry["name"] == "scratch" {
			writable = true
		}
		if entry["writable"] != writable {
			t.Errorf("volume %v writable=%v, want %v", entry["name"], entry["writable"], writable)
		}
	}
}

// TestWritableVolumeEndToEnd drives one writable claim through the whole
// stack: the SDK's mode reaches the engine as a writable attach, a live writer
// refuses every other claim on the name, and release unmounts before removal.
func TestWritableVolumeEndToEnd(t *testing.T) {
	scratch := writeVolumeImage(t, "scratch.img", "scratch-bytes")
	stack := startTenantStack(t, "node-token", nil,
		[]config.VolumeSpec{{Name: "scratch", Path: scratch, Writable: true}})

	sb, err := stack.client.New(t.Context(), "rt:24.04",
		sandbox.WithVolumes(sandbox.Volume{Name: "scratch", Mount: "/datasets/rw", Mode: "rw"}))
	if err != nil {
		t.Fatalf("writable claim: %v", err)
	}
	if want := []sandbox.Volume{{Name: "scratch", Mount: "/datasets/rw", Mode: "rw"}}; !slices.Equal(sb.Volumes, want) {
		t.Errorf("claim volumes %+v, want %+v", sb.Volumes, want)
	}
	applied := []string{"attach:scratch:rw", "mount:scratch:/datasets/rw:rw"}
	if got := stack.eng.volumeOpsLog(); !slices.Equal(got, applied) {
		t.Errorf("engine ops %v, want %v", got, applied)
	}
	for _, requested := range []string{`{"name":"scratch"}`, `{"name":"scratch","mode":"rw"}`} {
		if status, _ := rawClaim(t, stack, requested); status != http.StatusConflict {
			t.Errorf("claim %s under a live writer: %d, want 409", requested, status)
		}
	}

	if err := sb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	want := slices.Concat(applied, []string{"umount:/datasets/rw", "remove"})
	if got := stack.eng.volumeOpsLog(); !slices.Equal(got, want) {
		t.Errorf("engine ops after release %v, want %v", got, want)
	}
}

// TestVolumeModeWireShape pins the claim reply's volume bytes independently of
// the SDK mirror. The read-only leg runs second on purpose: it is admitted
// only because the writer's release cleared the dirty marker.
func TestVolumeModeWireShape(t *testing.T) {
	scratch := writeVolumeImage(t, "scratch.img", "scratch-bytes")
	stack := startTenantStack(t, "node-token", nil,
		[]config.VolumeSpec{{Name: "scratch", Path: scratch, Writable: true}})
	for _, tt := range []struct {
		name      string
		requested string
		want      map[string]any
	}{
		{
			"writable echoes its mode",
			`{"name":"scratch","mount":"/datasets/x","mode":"rw"}`,
			map[string]any{"name": "scratch", "mount": "/datasets/x", "mode": "rw"},
		},
		{
			"read-only omits mode",
			`{"name":"scratch","mount":"/datasets/x"}`,
			map[string]any{"name": "scratch", "mount": "/datasets/x"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status, claimed := rawClaim(t, stack, tt.requested)
			if status != http.StatusOK {
				t.Fatalf("claim: %d, want 200", status)
			}
			if len(claimed.Volumes) != 1 || !maps.Equal(claimed.Volumes[0], tt.want) {
				t.Errorf("reply volumes %v, want [%v]", claimed.Volumes, tt.want)
			}
			if err := stack.client.Attach(stack.addr, claimed.ID, claimed.Token).Close(); err != nil {
				t.Fatalf("release: %v", err)
			}
		})
	}
}

func TestClaimRefRoundTrip(t *testing.T) {
	stack := startStack(t, "node-token")
	sb, err := stack.client.New(t.Context(), "rt:24.04", sandbox.WithClaimRef("ns/workload"))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer sb.Close()

	list, err := stack.client.Sandboxes(t.Context())
	if err != nil {
		t.Fatalf("list sandboxes: %v", err)
	}
	i := slices.IndexFunc(list, func(s sandbox.SandboxSummary) bool { return s.ID == sb.ID })
	if i < 0 {
		t.Fatalf("claim %s missing from the index %+v", sb.ID, list)
	}
	if list[i].ClaimRef != "ns/workload" {
		t.Errorf("claim_ref %q, want ns/workload", list[i].ClaimRef)
	}
	if list[i].Key.Engine != sandbox.EngineCH {
		t.Errorf("engine %q, want the defaulted ch — the SDK key must carry the axis", list[i].Key.Engine)
	}
}

// TestAttachOnlyVolumeEndToEnd drives one attach-only writable claim through
// the whole stack: the device is attached writable and nothing else happens —
// no mount, no marker, no unmount at release — while admission still excludes
// every other claim on the name, which is what protects third parties.
func TestAttachOnlyVolumeEndToEnd(t *testing.T) {
	scratch := writeVolumeImage(t, "scratch.img", "scratch-bytes")
	stack := startTenantStack(t, "node-token", nil,
		[]config.VolumeSpec{{Name: "scratch", Path: scratch, Writable: true}})

	sb, err := stack.client.New(t.Context(), "rt:24.04",
		sandbox.WithVolumes(sandbox.Volume{Name: "scratch", Mode: "rw"}),
		sandbox.WithVolumesAttachOnly())
	if err != nil {
		t.Fatalf("attach-only claim: %v", err)
	}
	if want := []sandbox.Volume{{Name: "scratch", Mode: "rw"}}; !slices.Equal(sb.Volumes, want) {
		t.Errorf("claim volumes %+v, want %+v", sb.Volumes, want)
	}
	applied := []string{"attach:scratch:rw"}
	if got := stack.eng.volumeOpsLog(); !slices.Equal(got, applied) {
		t.Errorf("engine ops %v, want %v", got, applied)
	}
	assertNoDirtyMarker(t, scratch, "apply")
	for _, requested := range []string{`{"name":"scratch"}`, `{"name":"scratch","mode":"rw"}`} {
		if status, _ := rawClaim(t, stack, requested); status != http.StatusConflict {
			t.Errorf("claim %s under a live attach-only writer: %d, want 409", requested, status)
		}
	}

	if err := sb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	want := slices.Concat(applied, []string{"remove"})
	if got := stack.eng.volumeOpsLog(); !slices.Equal(got, want) {
		t.Errorf("engine ops after release %v, want %v", got, want)
	}
	assertNoDirtyMarker(t, scratch, "release")
}

// TestAttachOnlyVolumeWireShape pins both claim replies' volume bytes: an
// attach-only entry echoes without a mount, the eager entry is unchanged, and
// the request flag never rides back in either.
func TestAttachOnlyVolumeWireShape(t *testing.T) {
	scratch := writeVolumeImage(t, "scratch.img", "scratch-bytes")
	stack := startTenantStack(t, "node-token", nil,
		[]config.VolumeSpec{{Name: "scratch", Path: scratch, Writable: true}})
	for _, tt := range []struct {
		name    string
		request string
		want    map[string]any
	}{
		{
			"attach-only omits the mount",
			`{"template":"rt:24.04","volumes_attach_only":true,"volumes":[{"name":"scratch","mode":"rw"}]}`,
			map[string]any{"name": "scratch", "mode": "rw"},
		},
		{
			"eager claim is unchanged",
			`{"template":"rt:24.04","volumes":[{"name":"scratch","mode":"rw"}]}`,
			map[string]any{"name": "scratch", "mount": "/volumes/scratch", "mode": "rw"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status, body := rawJSON(t, stack, http.MethodPost, "/v1/claim", tt.request)
			if status != http.StatusOK {
				t.Fatalf("claim: %d %s, want 200", status, body)
			}
			var reply map[string]any
			if err := json.Unmarshal(body, &reply); err != nil {
				t.Fatalf("decode claim %s: %v", body, err)
			}
			if _, leaked := reply["volumes_attach_only"]; leaked {
				t.Errorf("reply %s carries the request flag", body)
			}
			entries, _ := reply["volumes"].([]any)
			if len(entries) != 1 {
				t.Fatalf("reply volumes %v, want one entry", reply["volumes"])
			}
			entry, _ := entries[0].(map[string]any)
			if !maps.Equal(entry, tt.want) {
				t.Errorf("reply volume %v, want %v", entry, tt.want)
			}
			var claimed rawClaimResponse
			if err := json.Unmarshal(body, &claimed); err != nil {
				t.Fatalf("decode claim %s: %v", body, err)
			}
			if err := stack.client.Attach(stack.addr, claimed.ID, claimed.Token).Close(); err != nil {
				t.Fatalf("release: %v", err)
			}
		})
	}
}

// TestDirtyVolumeRefusesReader: the marker a crashed writer leaves behind
// (pre-created here) turns read-only claims into 409s over the wire.
func TestDirtyVolumeRefusesReader(t *testing.T) {
	scratch := writeVolumeImage(t, "scratch.img", "scratch-bytes")
	if err := os.WriteFile(scratch+".dirty", nil, 0o600); err != nil {
		t.Fatalf("write dirty marker: %v", err)
	}
	stack := startTenantStack(t, "node-token", nil,
		[]config.VolumeSpec{{Name: "scratch", Path: scratch, Writable: true}})
	if status, _ := rawClaim(t, stack, `{"name":"scratch"}`); status != http.StatusConflict {
		t.Errorf("read-only claim on a dirty image: %d, want 409", status)
	}
}

type stack struct {
	client *sandbox.Client
	mgr    *pool.Manager
	eng    *fakeEngine
	addr   string
	token  string
}

func startStack(t *testing.T, apiToken string, pools ...config.PoolSpec) *stack {
	t.Helper()
	return startTenantStack(t, apiToken, nil, nil, pools...)
}

func startTenantStack(t *testing.T, apiToken string, tenants []config.TenantSpec, volumes []config.VolumeSpec, pools ...config.PoolSpec) *stack {
	t.Helper()
	// Short prefix: the sockets under it must fit darwin's 104-byte sun_path.
	dir, err := os.MkdirTemp("", "sbx")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	eng := newFakeEngine(dir)
	secrets, err := egress.NewSecretStore(nil)
	if err != nil {
		t.Fatalf("secrets: %v", err)
	}
	mgr, err := pool.NewManager(t.Context(), &config.Config{DataDir: dir, Pools: pools, Tenants: tenants, Volumes: volumes}, eng, secrets)
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	go mgr.Run(t.Context())

	ts := httptest.NewServer(server.New(apiToken, tenants, "", mgr, eng.real, nil, nil, nil, nil).Handler())
	t.Cleanup(ts.Close)
	addr := strings.TrimPrefix(ts.URL, "http://")
	client, err := sandbox.Connect(addr, sandbox.WithAPIToken(apiToken))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return &stack{client: client, mgr: mgr, eng: eng, addr: addr, token: apiToken}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met within 10s")
}

func writeVolumeImage(t *testing.T, name, content string) string {
	t.Helper()
	image := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(image, []byte(content), 0o600); err != nil {
		t.Fatalf("write volume image: %v", err)
	}
	return image
}

// assertNoDirtyMarker fails if the image carries the write-ahead marker: an
// attach-only claim makes no consistency promise, so it must never write one.
func assertNoDirtyMarker(t *testing.T, image, when string) {
	t.Helper()
	if _, err := os.Stat(image + ".dirty"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("dirty marker at %s: stat=%v, want no marker", when, err)
	}
}

// rawClaimResponse decodes the volume entries generically, so the assertion
// is the server's own JSON rather than the SDK's mirror of it.
type rawClaimResponse struct {
	ID      string           `json:"id"`
	Token   string           `json:"token"`
	Volumes []map[string]any `json:"volumes"`
}

func rawClaim(t *testing.T, st *stack, volume string) (int, rawClaimResponse) {
	t.Helper()
	status, body := rawJSON(t, st, http.MethodPost, "/v1/claim",
		fmt.Sprintf(`{"template":"rt:24.04","volumes":[%s]}`, volume))
	var claimed rawClaimResponse
	if status == http.StatusOK {
		if err := json.Unmarshal(body, &claimed); err != nil {
			t.Fatalf("decode claim %s: %v", body, err)
		}
	}
	return status, claimed
}

func rawJSON(t *testing.T, st *stack, method, route, body string) (int, []byte) {
	t.Helper()
	var payload io.Reader
	if body != "" {
		payload = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, "http://"+st.addr+route, payload)
	if err != nil {
		t.Fatalf("%s %s: %v", method, route, err)
	}
	req.Header.Set("Authorization", "Bearer "+st.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, route, err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s %s: %v", method, route, err)
	}
	return resp.StatusCode, out
}
