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
	resp, err := s.oneShot(ctx, &silkd.GitStatus{Path: path})
	if err != nil {
		return nil, err
	}
	st, ok := resp.(*silkd.GitStatusResult)
	if !ok {
		return nil, unexpected(resp)
	}
	return st, nil
}

// GitCommit stages nothing (call GitAdd first); it commits the index with
// message and author ("Name <email>") and returns the new commit hash.
func (s *Sandbox) GitCommit(ctx context.Context, path, message, author string) (string, error) {
	resp, err := s.oneShot(ctx, &silkd.GitCommit{Path: path, Message: message, Author: author})
	if err != nil {
		return "", err
	}
	c, ok := resp.(*silkd.GitCommitResult)
	if !ok {
		return "", unexpected(resp)
	}
	return c.Hash, nil
}

// GitAdd stages files under the repo at path.
func (s *Sandbox) GitAdd(ctx context.Context, path string, files ...string) error {
	return s.doneRPC(ctx, &silkd.GitAdd{Path: path, Files: files})
}
