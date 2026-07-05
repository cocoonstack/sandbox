package sandbox

import (
	"context"

	"github.com/cocoonstack/sandbox/sdk/silkd"
)

// Find returns the lines under path matching pattern (a regular expression);
// glob, when non-empty, narrows the walk to file names containing it.
func (s *Sandbox) Find(ctx context.Context, path, pattern, glob string) ([]silkd.Match, error) {
	return collectRPC[silkd.Match](ctx, s, &silkd.FsFind{Path: path, Pattern: pattern, Glob: glob})
}

// Replace rewrites pattern (a regular expression) to replacement in each of
// files, returning one result per file with its replacement count.
func (s *Sandbox) Replace(ctx context.Context, files []string, pattern, replacement string) ([]silkd.Replaced, error) {
	return collectRPC[silkd.Replaced](ctx, s, &silkd.FsReplace{Files: files, Pattern: pattern, Replacement: replacement})
}
