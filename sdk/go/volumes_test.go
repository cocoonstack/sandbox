package sandbox

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

func TestClientNewSendsVolumes(t *testing.T) {
	var got struct {
		Template string   `json:"template"`
		Volumes  []Volume `json:"volumes"`
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.UnmarshalRead(r.Body, &got); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_ = json.MarshalWrite(w, claimResponse{
			ID: "sb_1", Token: "tok", Volumes: []Volume{{Name: "weights-llama", Mount: "/models"}},
		})
	}))
	t.Cleanup(ts.Close)

	volumes := []Volume{{Name: "imagenet"}, {Name: "weights-llama", Mount: "/models"}}
	withVolumes := WithVolumes(volumes...)
	volumes[0].Name = "changed"
	sb, err := testClient(t, ts).New(t.Context(), "rt:24.04", withVolumes)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got.Template != "rt:24.04" {
		t.Errorf("template %q, want rt:24.04", got.Template)
	}
	if want := []Volume{{Name: "imagenet"}, {Name: "weights-llama", Mount: "/models"}}; !slices.Equal(got.Volumes, want) {
		t.Errorf("volumes %v, want %v", got.Volumes, want)
	}
	if want := []Volume{{Name: "weights-llama", Mount: "/models"}}; !slices.Equal(sb.Volumes, want) {
		t.Errorf("response volumes %v, want %v", sb.Volumes, want)
	}
}

func TestTemplateNewSendsVolumes(t *testing.T) {
	var got struct {
		Template   string   `json:"template"`
		Net        string   `json:"net"`
		Size       string   `json:"size"`
		Volumes    []Volume `json:"volumes"`
		NoRedirect bool     `json:"no_redirect"`
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.UnmarshalRead(r.Body, &got); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_ = json.MarshalWrite(w, claimResponse{
			ID: "sb_2", Token: "tok", Volumes: []Volume{{Name: "imagenet", Mount: "/datasets/imagenet"}},
		})
	}))
	t.Cleanup(ts.Close)

	c := testClient(t, ts)
	tpl := &Template{Name: "task:v1", c: c, addr: c.addr, net: "none", size: "small"}
	wantVolumes := []Volume{{Name: "imagenet", Mount: "/datasets/imagenet"}}
	sb, err := tpl.New(t.Context(), WithVolumes(wantVolumes...))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got.Template != "task:v1" || got.Net != "none" || got.Size != "small" || got.NoRedirect {
		t.Errorf("claim %+v, want redirectable task:v1/none/small", got)
	}
	if !slices.Equal(got.Volumes, wantVolumes) {
		t.Errorf("volumes %v, want %v", got.Volumes, wantVolumes)
	}
	if !slices.Equal(sb.Volumes, wantVolumes) {
		t.Errorf("response volumes %v, want %v", sb.Volumes, wantVolumes)
	}
}

func TestTemplateNewVolumeClaimFollowsRedirect(t *testing.T) {
	want := []Volume{{Name: "imagenet", Mount: "/datasets/imagenet"}}
	var got claimRequest
	var originAuth, targetAuth string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetAuth = r.Header.Get("Authorization")
		if err := json.UnmarshalRead(r.Body, &got); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_ = json.MarshalWrite(w, claimResponse{ID: "sb_3", Token: "tok", Volumes: want})
	}))
	t.Cleanup(target.Close)
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originAuth = r.Header.Get("Authorization")
		_ = json.MarshalWrite(w, claimResponse{
			Redirect: []string{strings.TrimPrefix(target.URL, "http://")}, RequirePromoted: true,
		})
	}))
	t.Cleanup(entry.Close)

	c := testClient(t, entry, WithAPIToken("tenant-token"))
	tpl := &Template{Name: "task:v1", c: c, addr: c.addr, net: "none", size: "small"}
	sb, err := tpl.New(t.Context(), WithVolumes(want...))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !got.NoRedirect || !got.RequirePromoted || !slices.Equal(got.Volumes, want) {
		t.Errorf("redirected claim = %+v, want promoted no_redirect with %v", got, want)
	}

	if originAuth != "Bearer tenant-token" || targetAuth != originAuth {
		t.Errorf("bearer at origin=%q at target=%q, want the caller's token on both hops", originAuth, targetAuth)
	}
	if !slices.Equal(sb.Volumes, want) {
		t.Errorf("response volumes = %+v, want %+v", sb.Volumes, want)
	}
}

func TestClientVolumes(t *testing.T) {
	imagenet := VolumeInfo{Name: "imagenet", DefaultMount: "/volumes/imagenet", SizeBytes: 42, Available: true, Nodes: 3}
	tests := []struct {
		name string
		want []VolumeInfo
	}{
		{"read-only catalog", []VolumeInfo{imagenet}},
		{"writable entry decodes", []VolumeInfo{imagenet, {Name: "scratch", DefaultMount: "/volumes/scratch", SizeBytes: 7, Available: true, Nodes: 1, Writable: true}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/v1/volumes" {
					t.Errorf("request = %s %s", r.Method, r.URL.Path)
				}
				if got := r.Header.Get("Authorization"); got != "Bearer sekret" {
					t.Errorf("authorization = %q", got)
				}
				_ = json.MarshalWrite(w, volumeListResponse{Volumes: tt.want})
			}))
			t.Cleanup(ts.Close)

			got, err := testClient(t, ts, WithAPIToken("sekret")).Volumes(t.Context())
			if err != nil {
				t.Fatalf("Volumes: %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("volumes = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestVolumeClaimRedirectPreservesEntries(t *testing.T) {
	want := []Volume{{Name: "imagenet", Mount: "/datasets/imagenet"}}
	var got claimRequest
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.UnmarshalRead(r.Body, &got); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_ = json.MarshalWrite(w, claimResponse{ID: "sb_3", Token: "tok", Volumes: want})
	}))
	t.Cleanup(target.Close)
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, claimResponse{
			Redirect: []string{strings.TrimPrefix(target.URL, "http://")},
		})
	}))
	t.Cleanup(entry.Close)

	sb, err := testClient(t, entry).New(t.Context(), "rt:24.04", WithVolumes(want...))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !got.NoRedirect || !slices.Equal(got.Volumes, want) {
		t.Errorf("redirected claim = %+v, want no_redirect with %v", got, want)
	}
	if !slices.Equal(sb.Volumes, want) {
		t.Errorf("response volumes = %+v, want %+v", sb.Volumes, want)
	}
}

func TestCheckpointNewRejectsVolumesLocally(t *testing.T) {
	ck := &Checkpoint{}
	sb, err := ck.New(t.Context(), WithVolumes(Volume{Name: "imagenet"}))
	if err == nil || !strings.Contains(err.Error(), "do not accept WithVolumes") {
		t.Errorf("err %v, want local WithVolumes rejection", err)
	}
	if sb != nil {
		t.Errorf("sandbox %+v, want nil", sb)
	}
}

func TestWithVolumesEncodesMode(t *testing.T) {
	tests := []struct {
		name string
		mode string
	}{
		{"empty stays omitted", ""},
		{"ro normalizes to omitted", "ro"},
		{"rw rides the wire", "rw"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var raw struct {
				Volumes []map[string]any `json:"volumes"`
			}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.UnmarshalRead(r.Body, &raw); err != nil {
					t.Errorf("decode body: %v", err)
				}
				_ = json.MarshalWrite(w, claimResponse{ID: "sb_1", Token: "tok"})
			}))
			t.Cleanup(ts.Close)
			if _, err := testClient(t, ts).New(t.Context(), "rt:24.04", WithVolumes(Volume{Name: "a", Mode: tt.mode})); err != nil {
				t.Fatalf("New: %v", err)
			}
			got, present := raw.Volumes[0]["mode"]
			if tt.mode == volumeModeRW {
				if !present || got != volumeModeRW {
					t.Errorf("wire mode = %v (present=%v), want %q", got, present, volumeModeRW)
				}
			} else if present {
				t.Errorf("wire mode = %v, want omitted", got)
			}
		})
	}
}

func TestWithVolumesAttachOnlySendsFlagAndDecodesMountlessEcho(t *testing.T) {
	var raw struct {
		Volumes           []map[string]any `json:"volumes"`
		VolumesAttachOnly bool             `json:"volumes_attach_only"`
	}
	echo := []Volume{{Name: "imagenet"}, {Name: "scratch", Mode: "rw"}}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.UnmarshalRead(r.Body, &raw); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_ = json.MarshalWrite(w, claimResponse{ID: "sb_1", Token: "tok", Volumes: echo})
	}))
	t.Cleanup(ts.Close)

	sb, err := testClient(t, ts).New(t.Context(), "rt:24.04",
		WithVolumes(Volume{Name: "imagenet"}, Volume{Name: "scratch", Mode: "rw"}),
		WithVolumesAttachOnly())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !raw.VolumesAttachOnly {
		t.Error("volumes_attach_only missing from the claim body")
	}
	for _, entry := range raw.Volumes {
		if mount, present := entry["mount"]; present {
			t.Errorf("volume %v carries mount %v, want none", entry["name"], mount)
		}
		if _, present := entry["attach_only"]; present {
			t.Error("legacy attach-only claim carries a per-entry flag")
		}
	}
	echo[0].AttachOnly, echo[1].AttachOnly = true, true
	if !slices.Equal(sb.Volumes, echo) {
		t.Errorf("volumes = %+v, want %+v", sb.Volumes, echo)
	}
}

func TestWithVolumesAttachOnlyRejectsMountLocally(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("server should not be contacted for a locally invalid claim")
	}))
	t.Cleanup(ts.Close)

	_, err := testClient(t, ts).New(t.Context(), "rt:24.04",
		WithVolumes(Volume{Name: "imagenet", Mount: "/datasets"}), WithVolumesAttachOnly())
	if err == nil || !strings.Contains(err.Error(), "meaningless with WithVolumesAttachOnly") {
		t.Errorf("err = %v, want local mount rejection", err)
	}
}

func TestRejectsInvalidVolumeModeLocally(t *testing.T) {
	tests := []struct {
		name  string
		claim func(ctx context.Context, c *Client) (*Sandbox, error)
	}{
		{"Client.New", func(ctx context.Context, c *Client) (*Sandbox, error) {
			return c.New(ctx, "rt:24.04", WithVolumes(Volume{Name: "a", Mode: "readwrite"}))
		}},
		{"Template.New", func(ctx context.Context, c *Client) (*Sandbox, error) {
			tpl := &Template{Name: "task:v1", c: c, addr: c.addr, net: "none", size: "small"}
			return tpl.New(ctx, WithVolumes(Volume{Name: "a", Mode: "bogus"}))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Error("server should not be contacted for a locally invalid mode")
			}))
			t.Cleanup(ts.Close)

			_, err := tt.claim(t.Context(), testClient(t, ts))
			if err == nil || !strings.Contains(err.Error(), "mode must be") {
				t.Errorf("err = %v, want local mode rejection", err)
			}
		})
	}
}

func TestSandboxVolumesEchoMode(t *testing.T) {
	want := []Volume{{Name: "imagenet", Mount: "/volumes/imagenet"}, {Name: "scratch", Mount: "/volumes/scratch", Mode: "rw"}}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, claimResponse{ID: "sb_1", Token: "tok", Volumes: want})
	}))
	t.Cleanup(ts.Close)

	sb, err := testClient(t, ts).New(t.Context(), "rt:24.04")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !slices.Equal(sb.Volumes, want) {
		t.Errorf("volumes = %+v, want %+v", sb.Volumes, want)
	}
}

func TestPerEntryAttachOnlyClaimCompatibility(t *testing.T) {
	for _, promoted := range []bool{false, true} {
		for _, mixed := range []bool{false, true} {
			t.Run(fmt.Sprintf("promoted=%t/mixed=%t", promoted, mixed), func(t *testing.T) {
				volumes := []Volume{{Name: "device-ro", AttachOnly: true}, {Name: "device-rw", Mode: "rw", AttachOnly: true}}
				if mixed {
					volumes = append(volumes, Volume{Name: "mounted-ro"})
				}
				var got claimRequest
				requests := 0
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests++
					if err := json.UnmarshalRead(r.Body, &got); err != nil {
						t.Errorf("decode claim: %v", err)
					}
					// An older server understands only the claim-wide flag.
					_ = json.MarshalWrite(w, claimResponse{ID: "sb_1", Token: "tok", Volumes: got.Volumes})
				}))
				t.Cleanup(ts.Close)
				c := testClient(t, ts)
				option := WithVolumes(volumes...)
				var sb *Sandbox
				var err error
				if promoted {
					tpl := &Template{Name: "task:v1", c: c, addr: c.addr, net: "none", size: "small"}
					sb, err = tpl.New(t.Context(), option)
				} else {
					sb, err = c.New(t.Context(), "rt:24.04", option)
				}
				if mixed {
					if err == nil || !strings.Contains(err.Error(), "claim without volumes") || requests != 0 {
						t.Fatalf("mixed claim reached server: requests=%d err=%v", requests, err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if !got.VolumesAttachOnly || !slices.Equal(sb.Volumes, volumes) {
					t.Fatalf("claim=%+v reply=%+v want=%+v", got, sb.Volumes, volumes)
				}
				for _, v := range got.Volumes {
					if v.AttachOnly {
						t.Fatal("claim retained per-entry attach_only")
					}
				}
				if _, err = c.New(t.Context(), "rt:24.04", option); err != nil || !got.VolumesAttachOnly {
					t.Fatalf("reused option lost raw intent: claim=%+v err=%v", got, err)
				}
			})
		}
	}
}

func TestPerEntryAttachOnlyRejectsClaimMount(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("invalid claim reached server") }))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	for _, promoted := range []bool{false, true} {
		var err error
		option := WithVolumes(Volume{Name: "data", Mount: "/data", AttachOnly: true})
		if promoted {
			tpl := &Template{Name: "task:v1", c: c, addr: c.addr}
			_, err = tpl.New(t.Context(), option)
		} else {
			_, err = c.New(t.Context(), "rt:24.04", option)
		}
		if err == nil || !strings.Contains(err.Error(), "meaningless with attach_only") {
			t.Fatalf("conflicting mount: %v", err)
		}
	}
}
