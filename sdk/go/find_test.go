package sandbox

import (
	"errors"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

func TestFindStreamsMatches(t *testing.T) {
	sb := fakeSandbox(t)
	ctx := t.Context()
	if err := sb.Mkdir(ctx, "/work", true); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := sb.WriteFile(ctx, "/work/a.rs", []byte("fn main() {\n// TODO fix\n}\n"), nil); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := sb.WriteFile(ctx, "/work/b.txt", []byte("TODO decoy\n"), nil); err != nil {
		t.Fatalf("write: %v", err)
	}

	matches, err := sb.Find(ctx, "/work", "TODO", "*.rs")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(matches) != 1 || !strings.HasSuffix(matches[0].File, "a.rs") {
		t.Fatalf("matches %+v, want the one .rs hit", matches)
	}
	if matches[0].Line != 2 || !strings.Contains(matches[0].Content, "TODO") {
		t.Errorf("match %+v, want line 2 with TODO", matches[0])
	}
}

func TestFindBadPatternIsTypedError(t *testing.T) {
	sb := fakeSandbox(t)
	_, err := sb.Find(t.Context(), "/", "(", "")
	var e *wire.ErrorResp
	if !errors.As(err, &e) || e.Kind != wire.KindBadRequest {
		t.Errorf("got %v, want bad_request error frame", err)
	}
}

func TestReplaceRewritesAndCounts(t *testing.T) {
	sb := fakeSandbox(t)
	ctx := t.Context()
	if err := sb.Mkdir(ctx, "/work", true); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := sb.WriteFile(ctx, "/work/r.txt", []byte("foo foo bar"), nil); err != nil {
		t.Fatalf("write: %v", err)
	}

	res, err := sb.Replace(ctx, []string{"/work/r.txt"}, "foo", "baz")
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if len(res) != 1 || res[0].Replacements != 2 {
		t.Fatalf("results %+v, want one file with 2 replacements", res)
	}
	got, err := sb.ReadFile(ctx, "/work/r.txt")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != "baz baz bar" {
		t.Errorf("content %q, want baz baz bar", got)
	}
}
