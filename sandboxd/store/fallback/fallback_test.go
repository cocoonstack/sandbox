package fallback_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/dir"
	"github.com/cocoonstack/sandbox/sandboxd/store/fallback"
	"github.com/cocoonstack/sandbox/sandboxd/store/s3"
	"github.com/cocoonstack/sandbox/sandboxd/store/storetest"
)

const id = "ck_00000000000000a1"

var errDown = fmt.Errorf("object ck/%s/meta.json: %w", id, errors.New("operation error S3: GetObject, StatusCode: 503, SlowDown"))

func TestWrapperPassesTheStoreContract(t *testing.T) {
	storetest.RunContract(t, fallback.New("checkpoints", newDir(t), newDir(t)))
}

func TestFallbackContractOverDirStores(t *testing.T) {
	primary, fb := newDir(t), newDir(t)
	storetest.RunFallbackContract(t, primary, fb, fallback.New("checkpoints", primary, fb))
}

func TestReadsAskTheFallbackOnlyAfterThePrimarysNotFound(t *testing.T) {
	reads := []struct {
		method string
		call   func(context.Context, *fallback.Store) (string, error)
		want   func(from string) string
	}{
		{"Fetch", func(ctx context.Context, w *fallback.Store) (string, error) {
			_, meta, _, err := w.Fetch(ctx, id)
			return string(meta), err
		}, func(from string) string { return from }},
		{"FetchFile", func(ctx context.Context, w *fallback.Store) (string, error) {
			return "", w.FetchFile(ctx, id, "disk.img", "dst")
		}, func(string) string { return "" }},
		{"ReadMeta", func(ctx context.Context, w *fallback.Store) (string, error) {
			meta, err := w.ReadMeta(ctx, id)
			return string(meta), err
		}, func(from string) string { return from }},
		{"ReadDigest", func(ctx context.Context, w *fallback.Store) (string, error) {
			return w.ReadDigest(ctx, id)
		}, func(from string) string { return from + "-digest" }},
	}
	for _, r := range reads {
		for _, tt := range []struct {
			name       string
			primary    func(*fake)
			want       string // the store whose answer comes back; "" for an error
			wantErr    error
			asksBackup bool
		}{
			{name: "primary holds it", primary: func(p *fake) { p.held[id] = []byte("primary") }, want: "primary"},
			{name: "primary not found", primary: func(*fake) {}, want: "fallback", asksBackup: true},
			{name: "primary 503", primary: func(p *fake) { p.errs[r.method] = errDown }, wantErr: errDown},
			{name: "primary timeout", primary: func(p *fake) { p.errs[r.method] = fmt.Errorf("get: %w", context.DeadlineExceeded) }, wantErr: context.DeadlineExceeded},
		} {
			t.Run(r.method+"/"+tt.name, func(t *testing.T) {
				p := newPair()
				p.fallback.held[id] = []byte("fallback")
				tt.primary(p.primary)
				got, err := r.call(t.Context(), p.wrapped)
				if tt.wantErr != nil {
					if !errors.Is(err, tt.wantErr) || errors.Is(err, store.ErrNotFound) {
						t.Fatalf("%s = %v, want %v and never store.ErrNotFound", r.method, err, tt.wantErr)
					}
				} else if err != nil || got != r.want(tt.want) {
					t.Fatalf("%s = %q, %v; want %q from the %s", r.method, got, err, r.want(tt.want), tt.want)
				}
				if asked := len(p.calls("fallback")) > 0; asked != tt.asksBackup {
					t.Errorf("fallback calls %v, want asked = %t", p.calls("fallback"), tt.asksBackup)
				}
			})
		}
	}
}

func TestAFetchStaysInThePrimaryWhileThePrimaryHoldsTheMeta(t *testing.T) {
	for _, tt := range []struct {
		name    string
		metaErr error
		wantErr error
	}{
		{name: "meta held, export missing", wantErr: store.ErrNotFound},
		{name: "meta check fails", metaErr: errDown, wantErr: errDown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := newPair()
			p.primary.held[id] = []byte("primary")
			p.primary.errs["Fetch"], p.primary.errs["FetchFile"], p.primary.errs["ReadMeta"] = store.ErrNotFound, store.ErrNotFound, tt.metaErr
			p.fallback.held[id] = []byte("fallback")
			if _, _, _, err := p.wrapped.Fetch(t.Context(), id); !errors.Is(err, tt.wantErr) {
				t.Errorf("Fetch = %v, want %v", err, tt.wantErr)
			}
			if err := p.wrapped.FetchFile(t.Context(), id, "disk.img", "dst"); !errors.Is(err, tt.wantErr) {
				t.Errorf("FetchFile = %v, want %v", err, tt.wantErr)
			}
			if calls := p.calls("fallback"); len(calls) > 0 {
				t.Errorf("fallback calls %v, want none: the primary's own record answers", calls)
			}
		})
	}
}

func TestAFallbackFailureIsNeverNotFound(t *testing.T) {
	for _, method := range []string{"Fetch", "FetchFile", "ReadMeta", "ReadDigest"} {
		t.Run(method, func(t *testing.T) {
			p := newPair()
			p.fallback.errs[method] = errDown
			var err error
			switch method {
			case "Fetch":
				_, _, _, err = p.wrapped.Fetch(t.Context(), id)
			case "FetchFile":
				err = p.wrapped.FetchFile(t.Context(), id, "disk.img", "dst")
			case "ReadMeta":
				_, err = p.wrapped.ReadMeta(t.Context(), id)
			case "ReadDigest":
				_, err = p.wrapped.ReadDigest(t.Context(), id)
			}
			if !errors.Is(err, errDown) || errors.Is(err, store.ErrNotFound) {
				t.Fatalf("%s = %v, want the fallback's failure, never store.ErrNotFound", method, err)
			}
		})
	}
}

func TestWritesReachOnlyThePrimary(t *testing.T) {
	p := newPair()
	ctx := t.Context()
	_, _ = p.wrapped.Stage(id)
	_ = p.wrapped.Publish(ctx, "staging", id, "")
	_, _, _ = p.wrapped.PrepareDigested(ctx, "staging", id, "") //nolint:dogsled // only the calls matter
	if calls := p.calls("fallback"); len(calls) > 0 {
		t.Errorf("fallback calls %v, want none", calls)
	}
	if calls := p.calls("primary"); len(calls) != 3 {
		t.Errorf("primary calls %v, want Stage, Publish and PrepareDigested", calls)
	}
}

func TestDeleteRemovesTheFallbackCopyFirst(t *testing.T) {
	for _, tt := range []struct {
		name    string
		setup   func(p pair)
		wantErr error
		want    []string
	}{
		{
			"both hold it", func(p pair) { p.fallback.held[id] = nil }, nil,
			[]string{"fallback.ReadMeta", "fallback.Delete", "primary.Delete"},
		},
		{
			"only the primary holds it", func(pair) {}, nil,
			[]string{"fallback.ReadMeta", "primary.Delete"},
		},
		{
			"the fallback check fails", func(p pair) { p.fallback.errs["ReadMeta"] = errDown }, errDown,
			[]string{"fallback.ReadMeta"},
		},
		{
			"the fallback delete fails", func(p pair) { p.fallback.held[id], p.fallback.errs["Delete"] = nil, errDown }, errDown,
			[]string{"fallback.ReadMeta", "fallback.Delete"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := newPair()
			p.primary.held[id] = nil
			tt.setup(p)
			if err := p.wrapped.Delete(t.Context(), id); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Delete = %v, want %v", err, tt.wantErr)
			}
			if got := p.methods(); !slices.Equal(got, tt.want) {
				t.Errorf("calls %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSetLabelsWriteBesideTheMeta(t *testing.T) {
	for _, tt := range []struct {
		name    string
		setup   func(p pair)
		wantErr error
		want    []string
	}{
		{
			"primary holds it", func(p pair) { p.primary.held[id], p.fallback.held[id] = nil, nil }, nil,
			[]string{"primary.ReadMeta", "primary.SetLabels"},
		},
		{
			"only the fallback holds it", func(p pair) { p.fallback.held[id] = nil }, nil,
			[]string{"primary.ReadMeta", "fallback.ReadMeta", "fallback.SetLabels"},
		},
		{
			"neither holds it", func(pair) {}, nil,
			[]string{"primary.ReadMeta", "fallback.ReadMeta", "primary.SetLabels"},
		},
		{
			"the primary check fails", func(p pair) { p.primary.errs["ReadMeta"], p.fallback.held[id] = errDown, nil }, errDown,
			[]string{"primary.ReadMeta"},
		},
		{
			"the fallback check fails", func(p pair) { p.fallback.errs["ReadMeta"] = errDown }, errDown,
			[]string{"primary.ReadMeta", "fallback.ReadMeta"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := newPair()
			tt.setup(p)
			if err := p.wrapped.SetLabels(t.Context(), id, []byte(`{"a":"1"}`)); !errors.Is(err, tt.wantErr) {
				t.Fatalf("SetLabels = %v, want %v", err, tt.wantErr)
			}
			if got := p.methods(); !slices.Equal(got, tt.want) {
				t.Errorf("calls %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMetasKeepsThePrimarysRecordAndFailsWhole(t *testing.T) {
	p := newPair()
	p.primary.held["ck_a"] = []byte("primary")
	p.fallback.held["ck_a"], p.fallback.held["ck_b"] = []byte("fallback"), []byte("fallback")
	recs, err := p.wrapped.Metas(t.Context(), "")
	slices.SortFunc(recs, func(a, b store.Record) int { return strings.Compare(a.ID, b.ID) })
	if err != nil || len(recs) != 2 || string(recs[0].Meta) != "primary" || recs[1].ID != "ck_b" {
		t.Fatalf("Metas = %+v, %v; want ck_a from the primary and ck_b from the fallback", recs, err)
	}
	for _, down := range []*fake{p.primary, p.fallback} {
		down.errs["Metas"] = errDown
		if recs, err = p.wrapped.Metas(t.Context(), ""); !errors.Is(err, errDown) {
			t.Errorf("Metas with the %s down = %+v, %v; want its failure, not a short list", down.name, recs, err)
		}
		down.errs["Metas"] = nil
	}
}

func TestSweepsReachBothStoresAndJoinTheirErrors(t *testing.T) {
	errPrimary, errFallback := errors.New("primary sweep"), errors.New("fallback sweep")
	for method, sweep := range map[string]func(*fallback.Store) error{
		"SweepStaging":     func(s *fallback.Store) error { return s.SweepStaging() },
		"SweepGenerations": func(s *fallback.Store) error { return s.SweepGenerations(t.Context()) },
	} {
		p := newPair()
		p.primary.errs[method], p.fallback.errs[method] = errPrimary, errFallback
		if err := sweep(p.wrapped); !errors.Is(err, errPrimary) || !errors.Is(err, errFallback) {
			t.Errorf("%s = %v, want both stores' failures", method, err)
		}
		if got := p.methods(); !slices.Equal(got, []string{"primary." + method, "fallback." + method}) {
			t.Errorf("%s calls %v, want the primary's and the fallback's", method, got)
		}
	}
}

func TestStatsCountEachFallbackAnswer(t *testing.T) {
	p := newPair()
	ctx := t.Context()
	p.fallback.held[id] = nil
	_, _ = p.wrapped.ReadMeta(ctx, id)
	_, _, _, _ = p.wrapped.Fetch(ctx, "ck_00000000000000ff") //nolint:dogsled // only the counters matter
	p.fallback.errs["ReadDigest"] = errDown
	_, _ = p.wrapped.ReadDigest(ctx, id)
	p.primary.held["ck_00000000000000fe"] = nil
	_ = p.wrapped.Delete(ctx, "ck_00000000000000fe")

	want := map[string]uint64{"read_meta/hit": 1, "fetch/miss": 1, "read_digest/error": 1, "delete/miss": 1}
	stats := p.wrapped.Stats()
	if len(stats) != 18 {
		t.Fatalf("Stats lists %d series, want every op and result, 18", len(stats))
	}
	for _, st := range stats {
		if key := st.Op + "/" + st.Result; st.Store != "checkpoints" || st.Count != want[key] {
			t.Errorf("%s/%s = %d, want %d", st.Store, key, st.Count, want[key])
		}
	}
}

// A primary s3.Store whose bucket fails in any way other than NoSuchKey must never be answered by the fallback, which here holds the record.
func TestPrimaryS3FailuresNeverReachTheFallback(t *testing.T) {
	for _, tt := range []struct {
		name     string
		endpoint func(t *testing.T) string
		timeout  time.Duration
	}{
		{"access denied", serve(403, "AccessDenied"), time.Minute},
		{"no such bucket", serve(404, "NoSuchBucket"), time.Minute},
		{"slow down after retries", serve(503, "SlowDown"), time.Minute},
		{"hangs past the caller's deadline", serve(0, ""), time.Second},
		{"unreachable", closedPort, time.Minute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fb := newDir(t)
			staging := storetest.StageRecord(t, fb, id, storetest.MetaJSON(id), "disk.img", "fallback")
			if err := fb.Publish(t.Context(), staging, id, ""); err != nil {
				t.Fatalf("seed the fallback: %v", err)
			}
			wrapped := fallback.New("checkpoints", newS3(t, tt.endpoint(t)), fb)
			for _, read := range []func(context.Context) error{
				func(ctx context.Context) error { _, err := wrapped.ReadMeta(ctx, id); return err },
				func(ctx context.Context) error { _, _, _, err := wrapped.Fetch(ctx, id); return err },
			} {
				ctx, cancel := context.WithTimeout(t.Context(), tt.timeout)
				err := read(ctx)
				cancel()
				if err == nil || errors.Is(err, store.ErrNotFound) {
					t.Errorf("read = %v, want the primary's failure, not the fallback's record or not found", err)
				}
			}
			for _, st := range wrapped.Stats() {
				if st.Count != 0 {
					t.Errorf("fallback asked: %+v", st)
				}
			}
		})
	}
}

// The S3 SDK names a 404 that carries no S3 error code NotFound, which s3.Store cannot tell from a missing key; docs/deploy.md states this exception.
func TestAPrimary404WithoutAnS3CodeReachesTheFallback(t *testing.T) {
	fb := newDir(t)
	staging := storetest.StageRecord(t, fb, id, storetest.MetaJSON(id), "disk.img", "fallback")
	if err := fb.Publish(t.Context(), staging, id, ""); err != nil {
		t.Fatalf("seed the fallback: %v", err)
	}
	wrapped := fallback.New("checkpoints", newS3(t, serve(404, "")(t)), fb)
	if meta, err := wrapped.ReadMeta(t.Context(), id); err != nil || string(meta) != storetest.MetaJSON(id) {
		t.Fatalf("ReadMeta = %q, %v; want the fallback's record", meta, err)
	}
}

func TestAFallbackS3OutageAfterANotFoundIsAnError(t *testing.T) {
	for _, tt := range []struct {
		name     string
		endpoint func(t *testing.T) string
	}{
		{"slow down after retries", serve(503, "SlowDown")},
		{"unreachable", closedPort},
	} {
		t.Run(tt.name, func(t *testing.T) {
			wrapped := fallback.New("checkpoints", newDir(t), newS3(t, tt.endpoint(t)))
			if _, err := wrapped.ReadMeta(t.Context(), id); err == nil || errors.Is(err, store.ErrNotFound) {
				t.Fatalf("ReadMeta = %v, want the fallback's failure, never not found", err)
			}
		})
	}
}

type pair struct {
	wrapped           *fallback.Store
	primary, fallback *fake
	mu                *sync.Mutex
	log               *[]string
}

func newPair() pair {
	var mu sync.Mutex
	var log []string
	mk := func(name string) *fake {
		return &fake{name: name, mu: &mu, log: &log, held: map[string][]byte{}, errs: map[string]error{}}
	}
	p := pair{primary: mk("primary"), fallback: mk("fallback"), mu: &mu, log: &log}
	p.wrapped = fallback.New("checkpoints", p.primary, p.fallback)
	return p
}

func (p pair) methods() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(*p.log))
	for i, c := range *p.log {
		out[i], _, _ = strings.Cut(c, " ")
	}
	return out
}

func (p pair) calls(name string) []string {
	return slices.DeleteFunc(p.methods(), func(c string) bool { return !strings.HasPrefix(c, name+".") })
}

// fake holds the metas in held; errs fails one method, store.ErrNotFound included.
type fake struct {
	name string
	mu   *sync.Mutex
	log  *[]string
	held map[string][]byte
	errs map[string]error
}

var _ store.Store = (*fake)(nil)

func (f *fake) call(method, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	*f.log = append(*f.log, f.name+"."+method+" "+id)
	return f.errs[method]
}

func (f *fake) lookup(method, id string) ([]byte, error) {
	if err := f.call(method, id); err != nil {
		return nil, err
	}
	meta, ok := f.held[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	if meta == nil {
		meta = []byte(f.name)
	}
	return meta, nil
}

func (f *fake) Stage(id string) (string, error) { return "", f.call("Stage", id) }

func (f *fake) Publish(_ context.Context, _, id, _ string) error { return f.call("Publish", id) }

func (f *fake) PrepareDigested(_ context.Context, _, id, _ string) (string, store.CommitFunc, error) {
	return "", nil, f.call("PrepareDigested", id)
}

func (f *fake) Fetch(_ context.Context, id string) (string, []byte, string, error) {
	meta, err := f.lookup("Fetch", id)
	if err != nil {
		return "", nil, "", err
	}
	return f.name + "-dir", meta, f.name + "-digest", nil
}

func (f *fake) FetchFile(_ context.Context, id, _, _ string) error {
	_, err := f.lookup("FetchFile", id)
	return err
}

func (f *fake) ReadMeta(_ context.Context, id string) ([]byte, error) {
	return f.lookup("ReadMeta", id)
}

func (f *fake) ReadDigest(_ context.Context, id string) (string, error) {
	if _, err := f.lookup("ReadDigest", id); err != nil {
		return "", err
	}
	return f.name + "-digest", nil
}

func (f *fake) Metas(_ context.Context, prefix string) ([]store.Record, error) {
	if err := f.call("Metas", prefix); err != nil {
		return nil, err
	}
	var recs []store.Record
	for _, id := range slices.Sorted(maps.Keys(f.held)) {
		recs = append(recs, store.Record{ID: id, Meta: f.held[id]})
	}
	return recs, nil
}

func (f *fake) SetLabels(_ context.Context, id string, _ []byte) error {
	return f.call("SetLabels", id)
}

func (f *fake) Delete(_ context.Context, id string) error { return f.call("Delete", id) }

func (f *fake) SweepStaging() error { return f.call("SweepStaging", "") }

func (f *fake) SweepGenerations(context.Context) error { return f.call("SweepGenerations", "") }

func newDir(t *testing.T) *dir.Store {
	t.Helper()
	st, err := dir.New(filepath.Join(t.TempDir(), "ck"), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("dir.New: %v", err)
	}
	return st
}

func newS3(t *testing.T, endpoint string) *s3.Store {
	t.Helper()
	t.Setenv("SBX_TEST_S3_KEY_ID", "test")
	t.Setenv("SBX_TEST_S3_SECRET", "test")
	st, err := s3.New(t.Context(), store.S3Config{
		Bucket: "testbucket", Prefix: "ck/", Endpoint: endpoint, Region: "us-east-1", ForcePathStyle: true,
		AccessKeyIDEnv: "SBX_TEST_S3_KEY_ID", SecretAccessKeyEnv: "SBX_TEST_S3_SECRET",
	}, t.TempDir(), store.CheckpointIDRe, true)
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	return st
}

// serve answers every request with an S3 error, with an empty body when code is empty, or, with status 0, holds it until the client hangs up or the test ends.
func serve(status int, code string) func(t *testing.T) string {
	return func(t *testing.T) string {
		release := make(chan struct{})
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if status == 0 {
				select {
				case <-r.Context().Done():
				case <-release:
				}
				return
			}
			if code == "" {
				w.WriteHeader(status)
				return
			}
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(status)
			_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>injected</Message></Error>", code)
		}))
		t.Cleanup(ts.Close)
		t.Cleanup(func() { close(release) }) // runs first, so Close never waits on a held request
		return ts.URL
	}
}

func closedPort(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return "http://" + addr
}

// TestFallbackContractRealEndpoint runs both contracts on two buckets of one S3 endpoint; the _PRIMARY_ and _FALLBACK_ key env vars hold keys scoped to one bucket each, and default to AWS_*.
func TestFallbackContractRealEndpoint(t *testing.T) {
	endpoint := os.Getenv("SANDBOX_S3_E2E")
	if endpoint == "" {
		t.Skip("SANDBOX_S3_E2E not set (export it to an S3 endpoint with two buckets to run)")
	}
	buckets := map[string]string{
		"PRIMARY":  cmp.Or(os.Getenv("SANDBOX_S3_E2E_BUCKET"), "sbx-checkpoints"),
		"FALLBACK": cmp.Or(os.Getenv("SANDBOX_S3_E2E_FALLBACK_BUCKET"), "sbx-checkpoints-fallback"),
	}
	scoped := os.Getenv("SANDBOX_S3_E2E_FALLBACK_ACCESS_KEY_ID") != ""
	for role := range buckets {
		idEnv, secretEnv := keyEnv(role)
		if os.Getenv(idEnv) == "" {
			t.Setenv(idEnv, os.Getenv("AWS_ACCESS_KEY_ID"))
			t.Setenv(secretEnv, os.Getenv("AWS_SECRET_ACCESS_KEY"))
		}
	}
	root := fmt.Sprintf("fallback-%d/", time.Now().UnixNano())
	open := func(t *testing.T, bucket, keyRole, prefix string) *s3.Store {
		t.Helper()
		idEnv, secretEnv := keyEnv(keyRole)
		st, err := s3.New(t.Context(), store.S3Config{
			Bucket: bucket, Prefix: root + prefix, Endpoint: endpoint, Region: "us-east-1", ForcePathStyle: true,
			UnsignedAcceptEncoding: os.Getenv("SANDBOX_S3_E2E_UNSIGNED_ACCEPT_ENCODING") != "",
			AccessKeyIDEnv:         idEnv, SecretAccessKeyEnv: secretEnv,
		}, t.TempDir(), store.CheckpointIDRe, true)
		if err != nil {
			t.Fatalf("s3.New %s: %v", bucket, err)
		}
		return st
	}
	t.Run("store contract", func(t *testing.T) {
		storetest.RunContract(t, fallback.New("checkpoints", open(t, buckets["PRIMARY"], "PRIMARY", "contract"), open(t, buckets["FALLBACK"], "FALLBACK", "contract")))
	})
	t.Run("fallback contract", func(t *testing.T) {
		primary, fb := open(t, buckets["PRIMARY"], "PRIMARY", "fallback"), open(t, buckets["FALLBACK"], "FALLBACK", "fallback")
		storetest.RunFallbackContract(t, primary, fb, fallback.New("checkpoints", primary, fb))
	})
	t.Run("each store signs with its own key", func(t *testing.T) {
		if !scoped {
			t.Skip("SANDBOX_S3_E2E_FALLBACK_ACCESS_KEY_ID not set: both stores share one key")
		}
		wrong := fallback.New("checkpoints", open(t, buckets["PRIMARY"], "FALLBACK", "keys"), open(t, buckets["FALLBACK"], "PRIMARY", "keys"))
		if _, err := wrong.ReadMeta(t.Context(), id); err == nil || errors.Is(err, store.ErrNotFound) {
			t.Fatalf("ReadMeta with the other bucket's key = %v, want the primary's refusal, never not found", err)
		}
	})
}

func keyEnv(role string) (id, secret string) {
	return "SANDBOX_S3_E2E_" + role + "_ACCESS_KEY_ID", "SANDBOX_S3_E2E_" + role + "_SECRET_ACCESS_KEY"
}
