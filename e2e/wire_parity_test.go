package e2e

import (
	"cmp"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/server"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const sdkDir = "../sdk/go"

var wirePairs = []struct {
	sdk    string
	sends  bool
	server any
	allow  []string
}{
	{"claimRequest", true, server.ClaimRequest{}, []string{"egress"}},
	{"claimResponse", false, server.ClaimResponse{}, nil},
	{"forkRequest", true, server.ForkRequest{}, []string{"claim_ref_prefix", "on_expire"}},
	{"forkResponse", false, server.ForkResponse{}, nil},
	{"renewRequest", true, server.RenewRequest{}, []string{"on_expire"}},
	{"renewResponse", false, server.RenewResponse{}, nil},
	{"promoteRequest", true, server.PromoteRequest{}, nil},
	{"promoteResponse", false, server.PromoteResponse{}, nil},
	{"previewRequest", true, server.PreviewRequest{}, nil},
	{"previewResponse", false, server.PreviewResponse{}, nil},
	{"checkpointRequest", true, server.CheckpointRequest{}, nil},
	{"checkpointResponse", false, server.CheckpointResponse{}, nil},
	{"checkpointClaimRequest", true, server.CheckpointClaimRequest{}, nil},
	{"checkpointListResponse", false, server.CheckpointListResponse{}, nil},
	{"checkpointRecord", false, types.Checkpoint{}, []string{"node", "key", "tenant", "policy_source", "no_egress", "guest_env", "archive"}},
	{"volumeListResponse", false, server.VolumeListResponse{}, nil},
	{"VolumeInfo", false, types.VolumeInfo{}, nil},
	{"ManagedVolumeInfo", false, types.ManagedVolumeInfo{}, nil},
	{"VolumeHolder", false, types.VolumeHolder{}, nil},
	{"VolumeCreate", true, types.VolumeCreate{}, nil},
	{"volumeResponse", false, server.VolumeResponse{}, nil},
	{"VolumeState", false, types.VolumeState{}, nil},
	{"SandboxVolumesResponse", false, types.SandboxVolumesResponse{}, nil},
	{"VolumePatch", true, types.VolumePatch{}, nil},
	{"Volume", true, types.Volume{}, nil},
	{"EnvVar", true, types.EnvVar{}, nil},
	{"EnvInject", true, types.EnvInject{}, nil},
	{"envBody", true, server.SandboxEnv{}, nil},
	{"envPatchBody", true, server.SandboxEnvPatch{}, nil},
	{"PoolKey", false, types.PoolKey{}, nil},
	{"NodeInfo", false, server.InfoResponse{}, nil},
	{"PoolStatus", false, pool.PoolInfo{}, nil},
	{"TemplateStatus", false, pool.TemplateInfo{}, nil},
	{"SandboxSummary", false, pool.SandboxSummary{}, []string{"token"}},
	{"sandboxListResponse", false, server.SandboxListResponse{}, nil},
	{"poolUpdate", true, server.PoolUpdateRequest{}, nil},
	{"PoolSpec", true, config.PoolSpec{}, []string{"egress", "warmup", "egress_upstream_env", "capture_trim", "storage", "replicate_seconds", "reader", "net", "size"}},
	{"tenantsUpdate", true, server.TenantsRequest{}, nil},
	{"tenantUpdate", true, server.TenantRequest{}, nil},
	{"TenantSpec", true, reflect.Zero(reflect.TypeOf(server.TenantsRequest{}.Tenants).Elem()).Interface(), []string{"token"}},
	{"TenantInfo", false, pool.TenantInfo{}, nil},
	{"TenantList", false, pool.TenantPage{}, nil},
}

func TestSDKStructsMatchServerWireShape(t *testing.T) {
	sdkFields := parseSDKFields(t)
	for _, tt := range wirePairs {
		t.Run(tt.sdk, func(t *testing.T) {
			got, ok := sdkFields[tt.sdk]
			if !ok {
				t.Fatalf("sdk struct %s not found in %s", tt.sdk, sdkDir)
			}
			want := serverFields(reflect.TypeOf(tt.server))
			for name, opts := range want {
				gotOpts, present := got[name]
				differs := !present || (tt.sends && gotOpts != opts)
				switch allowed := slices.Contains(tt.allow, name); {
				case differs && !allowed:
					t.Errorf("%s: field %q differs: sdk present=%t options=%q, server options=%q", tt.sdk, name, present, gotOpts, opts)
				case !differs && allowed:
					t.Errorf("%s: field %q is allowlisted but matches", tt.sdk, name)
				}
			}
			for name := range got {
				if _, present := want[name]; !present {
					t.Errorf("%s: field %q is in the SDK but not on the server", tt.sdk, name)
				}
			}
		})
	}
}

func serverFields(typ reflect.Type) map[string]string {
	fields := map[string]string{}
	for f := range typ.Fields() {
		name, opts := splitJSONTag(f.Tag.Get("json"))
		switch {
		case name == "-" && opts == "":
		case f.Anonymous && name == "" && f.Type.Kind() == reflect.Struct:
			maps.Copy(fields, serverFields(f.Type))
		case f.IsExported():
			fields[cmp.Or(name, f.Name)] = opts
		}
	}
	return fields
}

func parseSDKFields(t *testing.T) map[string]map[string]string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(sdkDir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]string{}
	embedded := map[string][]string{}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(src, func(n ast.Node) bool {
			spec, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}
			if st, ok := spec.Type.(*ast.StructType); ok {
				out[spec.Name.Name] = map[string]string{}
				for _, f := range st.Fields.List {
					if len(f.Names) == 0 {
						if id, ok := f.Type.(*ast.Ident); ok {
							embedded[spec.Name.Name] = append(embedded[spec.Name.Name], id.Name)
						}
					}
					if f.Tag == nil {
						continue
					}
					tag, _ := strconv.Unquote(f.Tag.Value)
					if name, opts := splitJSONTag(reflect.StructTag(tag).Get("json")); name != "" && name != "-" {
						out[spec.Name.Name][name] = opts
					}
				}
			}
			return true
		})
	}
	var flatten func(string) map[string]string
	flatten = func(name string) map[string]string {
		for _, base := range embedded[name] {
			maps.Copy(out[name], flatten(base))
		}
		return out[name]
	}
	for name := range out {
		flatten(name)
	}
	return out
}

func splitJSONTag(tag string) (name, opts string) {
	name, rest, _ := strings.Cut(tag, ",")
	options := strings.Split(rest, ",")
	slices.Sort(options)
	return name, strings.Join(options, ",")
}
