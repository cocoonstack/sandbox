package sandbox

import (
	"context"
	"iter"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

// Find returns the lines under path matching the regular expression pattern; a non-empty glob narrows the file names.
func (s *Sandbox) Find(ctx context.Context, path, pattern, glob string) ([]wire.Match, error) {
	return s.collectRPC[wire.Match](ctx, &wire.FsFind{Path: path, Pattern: pattern, Glob: glob})
}

// FindSeq streams the matches Find would collect; breaking out of the loop ends the guest walk.
func (s *Sandbox) FindSeq(ctx context.Context, path, pattern, glob string) iter.Seq2[wire.Match, error] {
	return s.streamRPC[wire.Match](ctx, &wire.FsFind{Path: path, Pattern: pattern, Glob: glob})
}

// Replace rewrites the regular expression pattern in each of files, returning one result per file.
func (s *Sandbox) Replace(ctx context.Context, files []string, pattern, replacement string) ([]wire.Replaced, error) {
	return s.collectRPC[wire.Replaced](ctx, &wire.FsReplace{Files: files, Pattern: pattern, Replacement: replacement})
}
