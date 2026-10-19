package e2e

import (
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
)

func TestManagedVolumeHTTPIdentityAndConditionalDelete(t *testing.T) {
	st := startManagedHTTPStack(t, "root-token")
	created := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/work", "t1-token", `{"size_gib":20}`, "")
	if created.status != http.StatusCreated {
		t.Fatalf("PV-M01 create status=%d body=%s, want 201", created.status, created.body)
	}
	volume := decodeManagedVolume(t, created.body)
	if volume.ID == "" || volume.Name != "work" || volume.Tenant != "t1" || volume.Kind != "managed" ||
		volume.SizeBytes != 20<<30 || volume.State != "ready" {
		t.Fatalf("PV-M01 unexpected resource projection: %+v", volume)
	}
	if created.etag != `"`+volume.ID+`"` {
		t.Fatalf("PV-M01 etag=%q, want resource identity", created.etag)
	}
	for _, body := range []string{`{"size_gib":20}`, `{"tenant":"t1","size_gib":20}`} {
		repeated := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/work", "t1-token", body, "")
		if repeated.status != http.StatusOK || decodeManagedVolume(t, repeated.body).ID != volume.ID {
			t.Fatalf("PV-M02 create retry status=%d body=%s", repeated.status, repeated.body)
		}
	}
	changed := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/work", "t1-token", `{"size_gib":21}`, "")
	if changed.status != http.StatusConflict {
		t.Fatalf("PV-M03 changed capacity status=%d, want 409", changed.status)
	}
	got := requestManagedVolume(t, st, http.MethodGet, "/v1/volumes/work", "t1-token", "", "")
	if got.status != http.StatusOK || got.etag != created.etag || decodeManagedVolume(t, got.body).ID != volume.ID {
		t.Fatalf("PV-M02 inspect status=%d body=%s", got.status, got.body)
	}
	for _, tc := range []struct {
		etag string
		want int
	}{
		{want: http.StatusPreconditionRequired},
		{etag: `"vol_00000000000000000000000000000000"`, want: http.StatusPreconditionFailed},
	} {
		refused := requestManagedVolume(t, st, http.MethodDelete, "/v1/volumes/work", "t1-token", "", tc.etag)
		if refused.status != tc.want {
			t.Fatalf("PV-M10 delete precondition status=%d, want %d", refused.status, tc.want)
		}
	}
	for range 2 {
		deleted := requestManagedVolume(t, st, http.MethodDelete, "/v1/volumes/work", "t1-token", "", created.etag)
		if deleted.status != http.StatusNoContent {
			t.Fatalf("PV-M09/PV-M10 delete/retry status=%d body=%s", deleted.status, deleted.body)
		}
	}
	replacement := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/work", "t1-token", `{"size_gib":20}`, "")
	if replacement.status != http.StatusCreated || decodeManagedVolume(t, replacement.body).ID == volume.ID {
		t.Fatalf("PV-M10 replacement lacks a fresh identity: status=%d body=%s", replacement.status, replacement.body)
	}
	stale := requestManagedVolume(t, st, http.MethodDelete, "/v1/volumes/work", "t1-token", "", created.etag)
	if stale.status != http.StatusNoContent {
		t.Fatalf("PV-M10 authorized old deletion replay status=%d, want 204", stale.status)
	}
	survivor := requestManagedVolume(t, st, http.MethodGet, "/v1/volumes/work", "t1-token", "", "")
	if survivor.status != http.StatusOK || decodeManagedVolume(t, survivor.body).ID != decodeManagedVolume(t, replacement.body).ID {
		t.Fatalf("PV-M10 stale delete changed replacement: status=%d body=%s", survivor.status, survivor.body)
	}
}

func TestManagedVolumeHTTPAuthorizationAndPrivacy(t *testing.T) {
	st := startManagedHTTPStack(t, "root-token")
	created := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/private", "t1-token", `{"size_gib":1}`, "")
	if created.status != http.StatusCreated {
		t.Fatalf("PV-M04 fixture create status=%d body=%s", created.status, created.body)
	}
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		foreign := requestManagedVolume(t, st, method, "/v1/volumes/private", "t2-token", `{"size_gib":1}`, created.etag)
		if foreign.status != http.StatusNotFound {
			t.Errorf("PV-M04/PV-M08 foreign %s status=%d, want 404", method, foreign.status)
		}
		if method == http.MethodGet {
			missing := requestManagedVolume(t, st, method, "/v1/volumes/unknown", "t2-token", "", "")
			if foreign.status != missing.status || string(foreign.body) != string(missing.body) {
				t.Errorf("PV-M04 foreign and unknown inspection differ: %d/%d", foreign.status, missing.status)
			}
		}
		assertManagedReplyPrivate(t, foreign.body)
	}
	listing := requestManagedVolume(t, st, http.MethodGet, "/v1/volumes", "t2-token", "", "")
	if listing.status != http.StatusOK || strings.Contains(string(listing.body), `"private"`) {
		t.Fatalf("PV-M04 foreign listing status=%d body=%s", listing.status, listing.body)
	}
	root := requestManagedVolume(t, st, http.MethodGet, "/v1/volumes/private", "root-token", "", "")
	if root.status != http.StatusOK || decodeManagedVolume(t, root.body).Tenant != "t1" {
		t.Fatalf("PV-M04 root inspection status=%d body=%s", root.status, root.body)
	}
	for _, token := range []string{"", "invalid-token"} {
		for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
			reply := requestManagedVolume(t, st, method, "/v1/volumes/private", token, `{"size_gib":1}`, created.etag)
			if reply.status != http.StatusUnauthorized {
				t.Errorf("PV-M05 invalid credential %s status=%d, want 401", method, reply.status)
			}
			assertManagedReplyPrivate(t, reply.body)
		}
	}
	claimStatus, claim := rawClaim(t, st, "")
	if claimStatus != http.StatusOK {
		t.Fatalf("PV-M05 sandbox credential fixture status=%d", claimStatus)
	}
	sandboxAttempt := requestManagedVolume(t, st, http.MethodGet, "/v1/volumes/private", claim.Token, "", "")
	if sandboxAttempt.status != http.StatusUnauthorized {
		t.Errorf("PV-M05 sandbox credential grants volume management: status=%d", sandboxAttempt.status)
	}
	released := requestManagedVolume(t, st, http.MethodPost, "/v1/sandboxes/"+claim.ID+"/release", claim.Token, "", "")
	if released.status != http.StatusNoContent {
		t.Fatalf("PV-M05 release credential fixture status=%d", released.status)
	}
	override := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/override", "t1-token", `{"size_gib":1,"tenant":"t2"}`, "")
	if override.status != http.StatusForbidden {
		t.Errorf("PV-M06 tenant override status=%d, want 403", override.status)
	}
	invalidRoot := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/root-owned", "root-token", `{"size_gib":1}`, "")
	if invalidRoot.status != http.StatusBadRequest {
		t.Errorf("PV-M06 root missing tenant status=%d, want 400", invalidRoot.status)
	}
	rootCreate := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/two", "root-token", `{"size_gib":1,"tenant":"t2"}`, "")
	if rootCreate.status != http.StatusCreated || decodeManagedVolume(t, rootCreate.body).Tenant != "t2" {
		t.Fatalf("PV-M06 root explicit tenant status=%d body=%s", rootCreate.status, rootCreate.body)
	}
	deleted := requestManagedVolume(t, st, http.MethodDelete, "/v1/volumes/private", "t1-token", "", created.etag)
	if deleted.status != http.StatusNoContent {
		t.Fatalf("PV-M09 fixture deletion status=%d body=%s", deleted.status, deleted.body)
	}
	foreignTombstone := requestManagedVolume(t, st, http.MethodDelete, "/v1/volumes/private", "t2-token", "", created.etag)
	if foreignTombstone.status != http.StatusNotFound {
		t.Fatalf("PV-M05/PV-M10 deletion ID incorrectly grants authority: status=%d", foreignTombstone.status)
	}
}

func TestManagedVolumeHTTPRejectsInputAndSerializesCreation(t *testing.T) {
	st := startManagedHTTPStack(t, "root-token")
	for _, body := range []string{
		`{}`, `{"size_gib":0}`, `{"size_gib":-1}`, `{"size_gib":1025}`,
		`{"size_gib":1.5}`, `{"size_gib":9223372036854775808}`,
		`{"size_gib":1,"unknown":true}`, `{"size_gib":1} {}`, `{"size_gib":`,
	} {
		reply := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/invalid", "t1-token", body, "")
		if reply.status != http.StatusBadRequest {
			t.Errorf("PV-M07 input %q status=%d, want 400", body, reply.status)
		}
	}
	oversized := `{"size_gib":1,"padding":"` + strings.Repeat("x", 1<<20) + `"}`
	tooLarge := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/oversized", "t1-token", oversized, "")
	if tooLarge.status != http.StatusBadRequest {
		t.Errorf("PV-M07 oversized body status=%d, want 400", tooLarge.status)
	}
	for _, name := range []string{".bad", "cocoon-root"} {
		reply := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/"+name, "t1-token", `{"size_gib":1}`, "")
		if reply.status != http.StatusBadRequest {
			t.Errorf("PV-M07/PV-M08 name %q status=%d, want 400", name, reply.status)
		}
	}
	const callers = 8
	replies := make([]managedHTTPReply, callers)
	var group sync.WaitGroup
	for i := range replies {
		group.Go(func() {
			replies[i] = requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/race", "t1-token", `{"size_gib":1}`, "")
		})
	}
	group.Wait()
	var id string
	var created int
	for _, reply := range replies {
		if reply.status != http.StatusOK && reply.status != http.StatusCreated {
			t.Fatalf("PV-M12 concurrent create status=%d body=%s", reply.status, reply.body)
		}
		volume := decodeManagedVolume(t, reply.body)
		if id != "" && volume.ID != id {
			t.Fatalf("PV-M12 competing creates returned different identities")
		}
		id = volume.ID
		if reply.status == http.StatusCreated {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("PV-M12 got %d creations, want exactly one", created)
	}
}

func TestManagedVolumeHTTPRequiresExplicitCredential(t *testing.T) {
	st := startManagedHTTPStack(t, "root-token")
	missing := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/implicit", "", `{"size_gib":1,"tenant":"t1"}`, "")
	if missing.status != http.StatusUnauthorized {
		t.Fatalf("PV-M05 legacy empty root configuration grants management without credentials: status=%d", missing.status)
	}
	for _, route := range []string{"/v1/volumes", "/v1/volumes/implicit"} {
		for _, token := range []string{"", "invalid-token"} {
			got := requestManagedVolume(t, st, http.MethodGet, route, token, "", "")
			if got.status != http.StatusUnauthorized {
				t.Fatalf("PV-M05 open legacy root configuration grants discovery: status=%d", got.status)
			}
		}
	}
}

type managedHTTPReply struct {
	status int
	body   []byte
	etag   string
}

type managedHTTPVolume struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Tenant    string `json:"tenant"`
	Kind      string `json:"kind"`
	State     string `json:"state"`
	SizeBytes int64  `json:"size_bytes"`
}

func startManagedHTTPStack(t *testing.T, rootToken string) *stack {
	t.Helper()
	var cfg config.Config
	body := fmt.Sprintf(`{"managed_volume_dir":%q}`, filepath.Join(t.TempDir(), "managed"))
	if err := json.Unmarshal([]byte(body), &cfg); err != nil {
		t.Fatal(err)
	}
	st := startConfigStack(t, rootToken, &cfg)
	if rootToken == "" {
		return st
	}
	if err := st.mgr.SetTenants(t.Context(), []tenants.TenantSpec{
		{Name: "t1", Token: "t1-token"}, {Name: "t2", Token: "t2-token"},
	}); err != nil {
		t.Fatalf("configure tenants: %v", err)
	}
	return st
}

func requestManagedVolume(t *testing.T, st *stack, method, route, token, body, etag string) managedHTTPReply {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, "http://"+st.addr+route, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	if etag != "" {
		req.Header.Set("If-Match", etag)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, route, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	assertManagedReplyPrivate(t, data)
	return managedHTTPReply{status: resp.StatusCode, body: data, etag: resp.Header.Get("ETag")}
}

func decodeManagedVolume(t *testing.T, body []byte) managedHTTPVolume {
	t.Helper()
	var reply struct {
		Volume managedHTTPVolume `json:"volume"`
	}
	if err := json.Unmarshal(body, &reply); err != nil {
		t.Fatalf("decode managed volume: %v", err)
	}
	return reply.Volume
}

func assertManagedReplyPrivate(t *testing.T, body []byte) {
	t.Helper()
	for _, secret := range []string{"root-token", "t1-token", "t2-token", "/tmp/", "disk.raw", "registry.json"} {
		if strings.Contains(string(body), secret) {
			t.Errorf("managed API exposed private host or credential detail")
		}
	}
}
