package sandbox

import (
	"context"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

// Find returns the lines under path matching pattern (a regular expression);
// glob, when non-empty, narrows the walk to file names matching it (`*` and
// `?` wildcards).
func (s *Sandbox) Find(ctx context.Context, path, pattern, glob string) ([]wire.Match, error) {
	return collectRPC[wire.Match](ctx, s, &wire.FsFind{Path: path, Pattern: pattern, Glob: glob})
}

// Replace rewrites pattern (a regular expression) to replacement in each of
// files, returning one result per file with its replacement count.
func (s *Sandbox) Replace(ctx context.Context, files []string, pattern, replacement string) ([]wire.Replaced, error) {
	return collectRPC[wire.Replaced](ctx, s, &wire.FsReplace{Files: files, Pattern: pattern, Replacement: replacement})
}
