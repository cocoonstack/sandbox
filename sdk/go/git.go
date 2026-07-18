package sandbox

import (
	"context"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

// GitClone clones url into path in the sandbox; depth > 0 makes a shallow
// clone. Auth, when non-empty, is a token sent as an in-memory Authorization
// header. Needs the egress lane.
func (s *Sandbox) GitClone(ctx context.Context, url, path, branch string, depth uint32, auth string) error {
	return s.doneRPC(ctx, &wire.GitClone{URL: url, Path: path, Branch: branch, Depth: depth, Auth: auth})
}

// GitStatus returns the structured status of the repo at path.
func (s *Sandbox) GitStatus(ctx context.Context, path string) (*wire.GitStatusResult, error) {
	return oneShotRPC[wire.GitStatusResult](ctx, s, &wire.GitStatus{Path: path})
}

// GitCommit stages nothing (call GitAdd first); it commits the index with
// message and author ("Name <email>") and returns the new commit hash.
func (s *Sandbox) GitCommit(ctx context.Context, path, message, author string) (string, error) {
	c, err := oneShotRPC[wire.GitCommitResult](ctx, s, &wire.GitCommit{Path: path, Message: message, Author: author})
	if err != nil {
		return "", err
	}
	return c.Hash, nil
}

// GitAdd stages files under the repo at path.
func (s *Sandbox) GitAdd(ctx context.Context, path string, files ...string) error {
	return s.doneRPC(ctx, &wire.GitAdd{Path: path, Files: files})
}

// GitPush pushes the current branch. Auth as in GitClone. Needs the egress
// lane; on the no-network lane it fails with a typed error pointing at Push.
func (s *Sandbox) GitPush(ctx context.Context, path, auth string) error {
	return s.doneRPC(ctx, &wire.GitPush{Path: path, Auth: auth})
}

// GitPull pulls the current branch; same lane rules as GitPush.
func (s *Sandbox) GitPull(ctx context.Context, path, auth string) error {
	return s.doneRPC(ctx, &wire.GitPull{Path: path, Auth: auth})
}

// GitBranches lists the repo's branches and the current one.
func (s *Sandbox) GitBranches(ctx context.Context, path string) (*wire.GitBranches, error) {
	return oneShotRPC[wire.GitBranches](ctx, s, &wire.GitBranch{Path: path, Action: wire.BranchList})
}

// GitCreateBranch creates branch name.
func (s *Sandbox) GitCreateBranch(ctx context.Context, path, name string) error {
	return s.doneRPC(ctx, &wire.GitBranch{Path: path, Action: wire.BranchCreate, Name: name})
}

// GitDeleteBranch force-deletes branch name.
func (s *Sandbox) GitDeleteBranch(ctx context.Context, path, name string) error {
	return s.doneRPC(ctx, &wire.GitBranch{Path: path, Action: wire.BranchDelete, Name: name})
}

// GitCheckout checks out branch name.
func (s *Sandbox) GitCheckout(ctx context.Context, path, name string) error {
	return s.doneRPC(ctx, &wire.GitBranch{Path: path, Action: wire.BranchCheckout, Name: name})
}
