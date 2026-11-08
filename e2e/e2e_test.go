// Package e2e drives the full host stack in one process with only cocoon and the guest faked.
package e2e

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/server"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	storedir "github.com/cocoonstack/sandbox/sandboxd/store/dir"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/tenants/file"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

const nodeToken = "node-token"

var testKey = types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall}

func TestClaimThenWarmClaimThenCloseThroughTheWholeStack(t *testing.T) {
	st := startStack(t, config.PoolSpec{PoolKey: testKey, Warm: 1})

	var sb *sandbox.Sandbox
	t.Run("first claim", func(t *testing.T) {
		var err error
		sb, err = st.client.New(t.Context(), "rt:24.04")
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
		waitFor(t, st.oneWarm)
		warmBefore := st.mgr.Counters().ClaimsWarm
		warm, err := st.client.New(t.Context(), "rt:24.04")
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer warm.Close()
		if got := st.mgr.Counters().ClaimsWarm; got != warmBefore+1 {
			t.Errorf("warm claims %d, want %d", got, warmBefore+1)
		}
		if out, err := warm.Exec(t.Context(), "echo", "warm"); err != nil || out != "warm\n" {
			t.Errorf("exec on warm claim: %q, %v", out, err)
		}
	})

	t.Run("close releases and revokes access", func(t *testing.T) {
		if sb == nil {
			t.Skip("first claim subtest failed")
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
	st := startStack(t)
	parent, err := st.client.New(t.Context(), "rt:24.04")
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
	st := startStack(t)
	parent, err := st.client.New(t.Context(), "rt:24.04")
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
	byName, nameErr := st.client.New(t.Context(), "e2e-tpl:1")
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
	if err := st.client.DeleteTemplate(t.Context(), "e2e-tpl:1"); err == nil ||
		!strings.Contains(err.Error(), "unknown template") {
		t.Errorf("second delete: %v, want unknown template", err)
	}
}

func TestRenewEndToEnd(t *testing.T) {
	st := startStack(t)
	sb, err := st.client.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer sb.Close()

	longer, err := sb.Renew(t.Context(), time.Hour)
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if left := time.Until(longer); left < 59*time.Minute || left > time.Hour {
		t.Errorf("an hour's renew left %v on the lease", left)
	}
	if !sb.Deadline.Equal(longer) {
		t.Errorf("handle deadline %v, want the grant %v", sb.Deadline, longer)
	}
	code, raw := rawJSON(t, st, http.MethodGet, "/v1/sandboxes/"+sb.ID, "")
	var held types.Sandbox
	if code != http.StatusOK || json.Unmarshal(raw, &held) != nil || !held.Deadline.Equal(longer) {
		t.Errorf("node holds %s (HTTP %d), want deadline %v", raw, code, longer)
	}

	shorter, err := sb.Renew(t.Context(), 2*time.Minute)
	if err != nil {
		t.Fatalf("shortening Renew: %v", err)
	}
	if !shorter.Before(longer) {
		t.Errorf("a two-minute renew granted %v, not before the hour's %v", shorter, longer)
	}
}

func TestCheckpointEndToEnd(t *testing.T) {
	st := startStack(t)
	src, err := st.client.New(t.Context(), "rt:24.04")
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

	ckpts, err := st.client.Checkpoints(t.Context())
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

func TestTwoTenantsKeepTheirOwnCapsCheckpointsAndNoOperatorSurface(t *testing.T) {
	specs := []tenants.TenantSpec{
		{Name: "acme", Token: "acme-tok", MaxClaims: 1},
		{Name: "beta", Token: "beta-tok"},
	}
	st := startTenantStack(t, specs, nil)
	acme, err := sandbox.Connect(st.addr, sandbox.WithAPIToken("acme-tok"))
	if err != nil {
		t.Fatalf("connect acme: %v", err)
	}
	beta, err := sandbox.Connect(st.addr, sandbox.WithAPIToken("beta-tok"))
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
		{st.client, []string{"acme-step", "beta-step"}},
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
	if _, err := st.client.Info(t.Context()); err != nil {
		t.Errorf("root on operator surface: %v", err)
	}
}

func TestRuntimeTenantEndToEnd(t *testing.T) {
	st := startStack(t)
	if err := st.client.PutTenant(t.Context(), sandbox.TenantSpec{Name: "u:42", Token: "u42-tok", MaxClaims: 1}); err != nil {
		t.Fatalf("PutTenant: %v", err)
	}
	user, err := sandbox.Connect(st.addr, sandbox.WithAPIToken("u42-tok"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	sb, err := user.New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("claim with a tenant added at runtime: %v", err)
	}
	defer sb.Close()
	if _, err = user.New(t.Context(), "rt:24.04"); err == nil || !strings.Contains(err.Error(), "429") {
		t.Errorf("second claim past the runtime cap: %v, want 429", err)
	}
	list, err := st.client.Tenants(t.Context(), "", 0)
	if err != nil || len(list.Tenants) != 1 || list.Tenants[0] != (sandbox.TenantInfo{Name: "u:42", MaxClaims: 1, Claims: 1}) {
		t.Fatalf("Tenants = %+v, %v", list, err)
	}

	if err = st.client.DeleteTenant(t.Context(), "u:42"); err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}
	if _, err = user.New(t.Context(), "rt:24.04"); err == nil || !strings.Contains(err.Error(), "401") {
		t.Errorf("claim with a removed tenant's token: %v, want 401", err)
	}
	if out, execErr := sb.Exec(t.Context(), "echo", "still-here"); execErr != nil || out != "still-here\n" {
		t.Errorf("exec on a removed tenant's claim: %q, %v", out, execErr)
	}
	if _, err = sb.Renew(t.Context(), time.Hour); err == nil || !strings.Contains(err.Error(), "403") {
		t.Errorf("renew of a removed tenant's claim: %v, want 403", err)
	}
	status, body := rawJSON(t, st, http.MethodGet, "/v1/sandboxes?tenant=u:42", "")
	if status != http.StatusOK || !strings.Contains(string(body), sb.ID) || !strings.Contains(string(body), `"tenant":"u:42"`) {
		t.Errorf("root listing of the removed tenant: %d %s", status, body)
	}
	if status, body := rawJSON(t, st, http.MethodGet, "/v1/tenants", ""); status != http.StatusOK || !strings.Contains(string(body), `{"name":"u:42","claims":1,"removed":true}`) || strings.Contains(string(body), "tok") {
		t.Errorf("tenant list after removal: %d %s", status, body)
	}
}

func TestWrongAPITokenRejected(t *testing.T) {
	st := startStack(t)

	bad, err := sandbox.Connect(st.addr, sandbox.WithAPIToken("wrong"))
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
	st := startTenantStack(t, nil,
		[]config.VolumeSpec{
			{Name: "dataset", Path: image, DirectIO: "off"},
			{Name: "scratch", Path: scratch, Writable: true},
		},
		config.PoolSpec{PoolKey: testKey, Warm: 1})
	waitFor(t, st.oneWarm)
	warmBefore := st.mgr.Counters().ClaimsWarm

	sb, err := st.client.New(t.Context(), "rt:24.04",
		sandbox.WithVolumes(sandbox.Volume{Name: "dataset", Mount: "/datasets/e2e"}))
	if err != nil {
		t.Fatalf("volume claim: %v", err)
	}
	defer sb.Close()
	if want := []sandbox.Volume{{Name: "dataset", Mount: "/datasets/e2e"}}; !slices.Equal(sb.Volumes, want) {
		t.Errorf("claim volumes %+v, want %+v", sb.Volumes, want)
	}
	if counters := st.mgr.Counters(); counters.ClaimsWarm != warmBefore+1 {
		infos, _ := st.mgr.Info()
		t.Errorf("counters=%+v pools=%+v, want warm claims %d", counters, infos, warmBefore+1)
	}

	infos, err := st.client.Volumes(t.Context())
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
	_, body := rawJSON(t, st, http.MethodGet, "/v1/volumes", "")
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

func TestWriterVolumeEndToEnd(t *testing.T) {
	for _, tt := range []struct {
		name     string
		volume   sandbox.Volume
		applied  []string
		released []string
		marker   bool
	}{
		{
			name:     "writable",
			volume:   sandbox.Volume{Name: "scratch", Mount: "/datasets/rw", Mode: "rw"},
			applied:  []string{"attach:scratch:rw", "mount:scratch:/datasets/rw:rw"},
			released: []string{"umount:/datasets/rw", "remove"},
		},
		{
			name:     "attach-only",
			volume:   sandbox.Volume{Name: "scratch", Mode: "rw", AttachOnly: true},
			applied:  []string{"attach:scratch:rw"},
			released: []string{"remove"},
			marker:   true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scratch := writeVolumeImage(t, "scratch.img", "scratch-bytes")
			st := startTenantStack(t, nil,
				[]config.VolumeSpec{{Name: "scratch", Path: scratch, Writable: true}})

			sb, err := st.client.New(t.Context(), "rt:24.04", sandbox.WithVolumes(tt.volume))
			if err != nil {
				t.Fatalf("%s claim: %v", tt.name, err)
			}
			if want := []sandbox.Volume{tt.volume}; !slices.Equal(sb.Volumes, want) {
				t.Errorf("claim volumes %+v, want %+v", sb.Volumes, want)
			}
			if got := st.eng.volumeOpsLog(); !slices.Equal(got, tt.applied) {
				t.Errorf("engine ops %v, want %v", got, tt.applied)
			}
			if tt.marker {
				assertNoDirtyMarker(t, scratch, "apply")
			}
			for _, requested := range []string{`{"name":"scratch"}`, `{"name":"scratch","mode":"rw"}`} {
				if status, _ := rawClaim(t, st, requested); status != http.StatusConflict {
					t.Errorf("claim %s under a live %s writer: %d, want 409", requested, tt.name, status)
				}
			}

			if err := sb.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			want := slices.Concat(tt.applied, tt.released)
			if got := st.eng.volumeOpsLog(); !slices.Equal(got, want) {
				t.Errorf("engine ops after release %v, want %v", got, want)
			}
			if tt.marker {
				assertNoDirtyMarker(t, scratch, "release")
			}
		})
	}
}

func TestVolumeWireShape(t *testing.T) {
	scratch := writeVolumeImage(t, "scratch.img", "scratch-bytes")
	st := startTenantStack(t, nil,
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
		{
			"attach-only omits the mount",
			`{"name":"scratch","mode":"rw","attach_only":true}`,
			map[string]any{"name": "scratch", "mode": "rw", "attach_only": true},
		},
		{
			"eager claim is unchanged",
			`{"name":"scratch","mode":"rw"}`,
			map[string]any{"name": "scratch", "mount": "/volumes/scratch", "mode": "rw"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status, claimed := rawClaim(t, st, tt.requested)
			if status != http.StatusOK {
				t.Fatalf("claim: %d, want 200", status)
			}
			if len(claimed.Volumes) != 1 || !maps.Equal(claimed.Volumes[0], tt.want) {
				t.Errorf("reply volumes %v, want [%v]", claimed.Volumes, tt.want)
			}
			if err := st.client.Attach(st.addr, claimed.ID, claimed.Token).Close(); err != nil {
				t.Fatalf("release: %v", err)
			}
		})
	}
}

func TestClaimRefRoundTrip(t *testing.T) {
	st := startStack(t)
	sb, err := st.client.New(t.Context(), "rt:24.04", sandbox.WithClaimRef("ns/workload"))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer sb.Close()

	list, err := st.client.Sandboxes(t.Context())
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
}

func TestClaimMetadataRoundTrip(t *testing.T) {
	st := startStack(t)
	sb, err := st.client.New(t.Context(), "rt:24.04", sandbox.WithMetadata(map[string]string{"team": "a"}))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer sb.Close()

	list, err := st.client.Sandboxes(t.Context())
	if err != nil {
		t.Fatalf("list sandboxes: %v", err)
	}
	i := slices.IndexFunc(list, func(s sandbox.SandboxSummary) bool { return s.ID == sb.ID })
	if i < 0 || list[i].Metadata["team"] != "a" || list[i].CPUCount != 1 || list[i].MemTotalBytes != 512<<20 {
		t.Fatalf("index %+v, want %s with metadata team=a, 1 CPU, 512 MiB", list, sb.ID)
	}
	for _, tt := range []struct {
		query string
		code  int
		hit   bool
	}{
		{"?metadata=team=a", http.StatusOK, true},
		{"?metadata=team=b", http.StatusOK, false},
		{"?metadata=team", http.StatusBadRequest, false},
	} {
		status, body := rawJSON(t, st, http.MethodGet, "/v1/sandboxes"+tt.query, "")
		if status != tt.code || strings.Contains(string(body), sb.ID) != tt.hit {
			t.Errorf("%s: %d %s, want %d with the claim listed=%t", tt.query, status, body, tt.code, tt.hit)
		}
	}
	status, body := rawJSON(t, st, http.MethodPost, "/v1/claim", `{"template":"rt:24.04","metadata":{"a&b":"v"}}`)
	if status != http.StatusBadRequest || !strings.Contains(string(body), "metadata key") {
		t.Errorf("claim with a bad key: %d %s, want 400 naming the key bound", status, body)
	}
}

func TestClaimEnvEndToEnd(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Header.Get("Authorization"))
	}))
	t.Cleanup(origin.Close)
	st := startConfigStack(t, &config.Config{
		Secrets:             []egress.SecretSpec{{Name: "GW_KEY", Header: "Authorization"}},
		EgressInternalAllow: []string{"127.0.0.1/32"},
		Pools:               []config.PoolSpec{{PoolKey: testKey, Warm: 2, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "127.0.0.1", Secret: "GW_KEY"}}}}},
	})
	claim := func(key string) (string, string, *http.Client) {
		t.Helper()
		status, body := rawJSON(t, st, http.MethodPost, "/v1/claim",
			`{"template":"rt:24.04","env":{"GW_KEY":{"value":"`+key+`","guest":false},"AGENT_MODE":{"value":"on"}}}`)
		var claimed rawClaimResponse
		if status != http.StatusOK || json.Unmarshal(body, &claimed) != nil {
			t.Fatalf("claim: %d %s", status, body)
		}
		sock, err := st.mgr.AgentSocket(claimed.ID, pool.Cred{Token: claimed.Token})
		if err != nil {
			t.Fatalf("socket: %v", err)
		}
		door := engine.EgressSocketPath(sock)
		return claimed.ID, sock, &http.Client{Transport: &http.Transport{
			Proxy: http.ProxyURL(&url.URL{Scheme: "http", Host: "door"}),
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", door)
			},
		}}
	}
	seen := func(client *http.Client) string {
		t.Helper()
		resp, err := client.Get(origin.URL)
		if err != nil {
			t.Fatalf("egress: %v", err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("egress body: %v", err)
		}
		return string(body)
	}

	idA, sockA, a := claim("Bearer tenant-a")
	_, _, b := claim("Bearer tenant-b")
	if gotA, gotB := seen(a), seen(b); gotA != "Bearer tenant-a" || gotB != "Bearer tenant-b" {
		t.Fatalf("injected %q / %q, want each claim's own value", gotA, gotB)
	}
	if guestEnv := st.eng.guestEnv(sockA); guestEnv != "AGENT_MODE=\"on\"\n" {
		t.Errorf("guest env file %q, want only the guest entry", guestEnv)
	}
	if status, body := rawJSON(t, st, http.MethodPut, "/v1/sandboxes/"+idA+"/env", `{"env":{"GW_KEY":{"value":"Bearer rotated","guest":false}}}`); status != http.StatusNoContent {
		t.Fatalf("put env: %d %s", status, body)
	}
	if got := seen(a); got != "Bearer rotated" {
		t.Errorf("after PUT: injected %q, want Bearer rotated", got)
	}
	status, body := rawJSON(t, st, http.MethodGet, "/v1/sandboxes/"+idA+"/env", "")
	if status != http.StatusOK || string(body) != `{"env":{"GW_KEY":{"value":"","guest":false}}}` {
		t.Errorf("read env: %d %s, want the host-only name without its value", status, body)
	}
	for _, route := range []string{"/v1/sandboxes/" + idA, "/v1/sandboxes"} {
		if status, body := rawJSON(t, st, http.MethodGet, route, ""); status != http.StatusOK || strings.Contains(string(body), "Bearer") {
			t.Errorf("GET %s: %d %s, want 200 without a host-only value", route, status, body)
		}
	}
	if status, _ := rawJSON(t, st, http.MethodPut, "/v1/sandboxes/"+idA+"/env", `{"env":{"BAD NAME":{"value":"x"}}}`); status != http.StatusBadRequest {
		t.Errorf("bad name: %d, want 400", status)
	}
}

func TestClaimEnvThroughTheSDK(t *testing.T) {
	st := startStack(t, config.PoolSpec{PoolKey: testKey, Warm: 1})
	guestFile := func(sb *sandbox.Sandbox) string {
		t.Helper()
		sock, err := st.mgr.AgentSocket(sb.ID, pool.Cred{Token: sb.Token()})
		if err != nil {
			t.Fatalf("socket: %v", err)
		}
		return st.eng.guestEnv(sock)
	}
	readEnv := func(sb *sandbox.Sandbox, want map[string]sandbox.EnvVar) {
		t.Helper()
		got, err := sb.Env(t.Context())
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("Env = %v, %v; want %v", got, err, want)
		}
	}
	sb, err := st.client.New(t.Context(), "rt:24.04", sandbox.WithEnv(map[string]sandbox.EnvVar{
		"MODE": {Value: "on"}, "GW_KEY": {Value: "Bearer a", Guest: new(false)},
	}))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer sb.Close()
	if got := guestFile(sb); got != "MODE=\"on\"\n" {
		t.Errorf("guest env file %q after the claim, want only MODE", got)
	}
	readEnv(sb, map[string]sandbox.EnvVar{"MODE": {Value: "on"}, "GW_KEY": {Guest: new(false)}})

	if err = sb.PatchEnv(t.Context(), map[string]*sandbox.EnvVar{"MODE": nil, "STAGE": {Value: "2"}}); err != nil {
		t.Fatalf("PatchEnv: %v", err)
	}
	if got := guestFile(sb); got != "STAGE=\"2\"\n" {
		t.Errorf("guest env file %q after the patch, want only STAGE", got)
	}
	readEnv(sb, map[string]sandbox.EnvVar{"STAGE": {Value: "2"}, "GW_KEY": {Guest: new(false)}})

	if err = sb.SetEnv(t.Context(), nil); err != nil {
		t.Fatalf("SetEnv(nil): %v", err)
	}
	if got := guestFile(sb); got != "" {
		t.Errorf("guest env file %q after clearing", got)
	}
	readEnv(sb, map[string]sandbox.EnvVar{})

	ckpt, err := sb.Checkpoint(t.Context(), "env")
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	branch, err := ckpt.New(t.Context(), sandbox.WithEnv(map[string]sandbox.EnvVar{"BRANCH": {Value: "b"}}))
	if err != nil {
		t.Fatalf("branch: %v", err)
	}
	defer branch.Close()
	if got := guestFile(branch); got != "BRANCH=\"b\"\n" {
		t.Errorf("branch guest env file %q, want only BRANCH", got)
	}
	readEnv(branch, map[string]sandbox.EnvVar{"BRANCH": {Value: "b"}})
}

func TestForkClaimRefPrefixWireShape(t *testing.T) {
	st := startStack(t)
	status, body := rawJSON(t, st, http.MethodPost, "/v1/claim", `{"template":"rt:24.04"}`)
	if status != http.StatusOK {
		t.Fatalf("claim: %d %s", status, body)
	}
	var parent rawClaimResponse
	if err := json.Unmarshal(body, &parent); err != nil {
		t.Fatalf("decode claim %s: %v", body, err)
	}

	fork := func(extra string) []rawClaimResponse {
		t.Helper()
		status, body := rawJSON(t, st, http.MethodPost, "/v1/sandboxes/"+parent.ID+"/fork",
			fmt.Sprintf(`{"token":%q,"count":2,"ttl_seconds":60%s}`, parent.Token, extra))
		if status != http.StatusOK {
			t.Fatalf("fork: %d %s", status, body)
		}
		var resp struct {
			Children []rawClaimResponse `json:"children"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("decode fork %s: %v", body, err)
		}
		if len(resp.Children) != 2 {
			t.Fatalf("fork children %s, want 2", body)
		}
		return resp.Children
	}
	row := func(route string) map[string]any {
		t.Helper()
		status, body := rawJSON(t, st, http.MethodGet, route, "")
		if status != http.StatusOK {
			t.Fatalf("GET %s: %d %s", route, status, body)
		}
		var out map[string]any
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatalf("decode %s: %v", body, err)
		}
		return out
	}

	for _, child := range fork(`,"claim_ref_prefix":"ns/"`) {
		want := "ns/" + child.ID
		list, ok := row("/v1/sandboxes?claim_ref=" + url.QueryEscape(want))["sandboxes"].([]any)
		if !ok || len(list) != 1 {
			t.Fatalf("claim_ref=%s lists %v, want exactly child %s", want, list, child.ID)
		}
		listed, _ := list[0].(map[string]any)
		if listed["id"] != child.ID || listed["claim_ref"] != want {
			t.Errorf("claim_ref=%s row %v, want id %s claim_ref %s", want, listed, child.ID, want)
		}
		if got := row("/v1/sandboxes/" + child.ID)["claim_ref"]; got != want {
			t.Errorf("by-id claim_ref %v, want %s", got, want)
		}
	}
	for _, child := range fork("") {
		if got, ok := row("/v1/sandboxes/" + child.ID)["claim_ref"]; ok {
			t.Errorf("unprefixed fork child %s carries claim_ref %v", child.ID, got)
		}
	}
}

func TestDirtyVolumeRefusesReader(t *testing.T) {
	scratch := writeVolumeImage(t, "scratch.img", "scratch-bytes")
	if err := os.WriteFile(scratch+".dirty", nil, 0o600); err != nil {
		t.Fatalf("write dirty marker: %v", err)
	}
	st := startTenantStack(t, nil,
		[]config.VolumeSpec{{Name: "scratch", Path: scratch, Writable: true}})
	if status, _ := rawClaim(t, st, `{"name":"scratch"}`); status != http.StatusConflict {
		t.Errorf("read-only claim on a dirty image: %d, want 409", status)
	}
}

type rawClaimResponse struct {
	ID      string           `json:"id"`
	Token   string           `json:"token"`
	Volumes []map[string]any `json:"volumes"`
}

type stack struct {
	client *sandbox.Client
	mgr    *pool.Manager
	eng    *fakeEngine
	addr   string
	token  string
}

func (s *stack) oneWarm() bool {
	infos, _ := s.mgr.Info()
	return len(infos) == 1 && infos[0].Warm >= 1
}

func startStack(t *testing.T, pools ...config.PoolSpec) *stack {
	t.Helper()
	return startTenantStack(t, nil, nil, pools...)
}

func startTenantStack(t *testing.T, specs []tenants.TenantSpec, volumes []config.VolumeSpec, pools ...config.PoolSpec) *stack {
	t.Helper()
	st := startConfigStack(t, &config.Config{Pools: pools, Volumes: volumes})
	if len(specs) > 0 {
		if err := st.mgr.SetTenants(t.Context(), specs); err != nil {
			t.Fatalf("set tenants: %v", err)
		}
	}
	return st
}

func startConfigStack(t *testing.T, cfg *config.Config) *stack {
	t.Helper()

	dir, err := os.MkdirTemp("", "sbx")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	eng := newFakeEngine(dir)
	secrets, err := egress.NewSecretStore(cfg.Secrets)
	if err != nil {
		t.Fatalf("secrets: %v", err)
	}
	cfg.DataDir, cfg.APIToken = dir, nodeToken
	ckpts, err := storedir.New(filepath.Join(dir, "checkpoints"), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("checkpoint store: %v", err)
	}
	tpls, err := storedir.New(filepath.Join(dir, "checkpoints"), store.TemplateIDRe)
	if err != nil {
		t.Fatalf("template store: %v", err)
	}
	src, err := file.Open(t.Context(), dir, tenants.TokenSHA256(nodeToken))
	if err != nil {
		t.Fatalf("tenant set: %v", err)
	}
	mgr, err := pool.NewManager(t.Context(), cfg, eng, secrets, pool.Backends{Checkpoints: ckpts, Templates: tpls, Tenants: src})
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	go mgr.Run(t.Context())

	ts := httptest.NewServer(server.New(nodeToken, "", mgr, eng.real, nil, nil, nil, nil).Handler())
	t.Cleanup(ts.Close)
	addr := strings.TrimPrefix(ts.URL, "http://")
	client, err := sandbox.Connect(addr, sandbox.WithAPIToken(nodeToken))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return &stack{client: client, mgr: mgr, eng: eng, addr: addr, token: nodeToken}
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

func assertNoDirtyMarker(t *testing.T, image, when string) {
	t.Helper()
	if _, err := os.Stat(image + ".dirty"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("dirty marker at %s: stat=%v, want no marker", when, err)
	}
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
