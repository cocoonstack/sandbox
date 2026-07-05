package sandbox

import (
	"context"

	"github.com/cocoonstack/sandbox/sdk/silkd"
)

// GitClone clones url into path in the sandbox. Auth, when non-empty, is a
// token sent as an in-memory Authorization header. Needs the egress lane.
func (s *Sandbox) GitClone(ctx context.Context, url, path, branch, auth string) error {
	return s.doneRPC(ctx, &silkd.GitClone{URL: url, Path: path, Branch: branch, Auth: auth})
}

// GitStatus returns the structured status of the repo at path.
func (s *Sandbox) GitStatus(ctx context.Context, path string) (*silkd.GitStatusResult, error) {
	return oneShotRPC[silkd.GitStatusResult](ctx, s, &silkd.GitStatus{Path: path})
}

// GitCommit stages nothing (call GitAdd first); it commits the index with
// message and author ("Name <email>") and returns the new commit hash.
func (s *Sandbox) GitCommit(ctx context.Context, path, message, author string) (string, error) {
	c, err := oneShotRPC[silkd.GitCommitResult](ctx, s, &silkd.GitCommit{Path: path, Message: message, Author: author})
	if err != nil {
		return "", err
	}
	return c.Hash, nil
}

// GitAdd stages files under the repo at path.
func (s *Sandbox) GitAdd(ctx context.Context, path string, files ...string) error {
	return s.doneRPC(ctx, &silkd.GitAdd{Path: path, Files: files})
}

// GitPush pushes the current branch. Auth as in GitClone. Needs the egress
// lane; on the no-network lane it fails with a typed error pointing at Push.
func (s *Sandbox) GitPush(ctx context.Context, path, auth string) error {
	return s.doneRPC(ctx, &silkd.GitPush{Path: path, Auth: auth})
}

// GitPull pulls the current branch; same lane rules as GitPush.
func (s *Sandbox) GitPull(ctx context.Context, path, auth string) error {
	return s.doneRPC(ctx, &silkd.GitPull{Path: path, Auth: auth})
}

// GitBranches lists the repo's branches and the current one.
func (s *Sandbox) GitBranches(ctx context.Context, path string) (*silkd.GitBranches, error) {
	return oneShotRPC[silkd.GitBranches](ctx, s, &silkd.GitBranch{Path: path, Action: silkd.BranchList})
}

// GitCreateBranch creates branch name.
func (s *Sandbox) GitCreateBranch(ctx context.Context, path, name string) error {
	return s.doneRPC(ctx, &silkd.GitBranch{Path: path, Action: silkd.BranchCreate, Name: name})
}

// GitDeleteBranch force-deletes branch name.
func (s *Sandbox) GitDeleteBranch(ctx context.Context, path, name string) error {
	return s.doneRPC(ctx, &silkd.GitBranch{Path: path, Action: silkd.BranchDelete, Name: name})
}

// GitCheckout checks out branch name.
func (s *Sandbox) GitCheckout(ctx context.Context, path, name string) error {
	return s.doneRPC(ctx, &silkd.GitBranch{Path: path, Action: silkd.BranchCheckout, Name: name})
}
