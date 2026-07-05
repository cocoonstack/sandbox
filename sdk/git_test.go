package sandbox

import (
	"slices"
	"testing"
)

func TestGitBranchLifecycle(t *testing.T) {
	sb := fakeSandbox(t)
	ctx := t.Context()

	if err := sb.GitCreateBranch(ctx, "/repo", "feature"); err != nil {
		t.Fatalf("create: %v", err)
	}
	br, err := sb.GitBranches(ctx, "/repo")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if br.Current != "main" || !slices.Contains(br.Branches, "feature") {
		t.Fatalf("branches %+v, want feature alongside current main", br)
	}

	if err = sb.GitCheckout(ctx, "/repo", "feature"); err != nil {
		t.Fatalf("checkout: %v", err)
	}
	if br, err = sb.GitBranches(ctx, "/repo"); err != nil || br.Current != "feature" {
		t.Fatalf("after checkout: current=%q err=%v", br.Current, err)
	}
	if err = sb.GitCheckout(ctx, "/repo", "nope"); err == nil {
		t.Error("checkout of a missing branch succeeded")
	}

	if err = sb.GitDeleteBranch(ctx, "/repo", "main"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if br, err = sb.GitBranches(ctx, "/repo"); err != nil || slices.Contains(br.Branches, "main") {
		t.Errorf("after delete: %+v err=%v", br, err)
	}
}

func TestGitPushPullPlumbing(t *testing.T) {
	sb := fakeSandbox(t)
	if err := sb.GitPush(t.Context(), "/repo", ""); err != nil {
		t.Errorf("push: %v", err)
	}
	if err := sb.GitPull(t.Context(), "/repo", "tok"); err != nil {
		t.Errorf("pull: %v", err)
	}
}
