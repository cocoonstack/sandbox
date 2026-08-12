// Package filecache keeps a sandbox's workspace directory in sync with a
// shared NAS workspace under a session-granular, multi-writer contract.
//
// The guest workspace lives on the sandbox's own local disk, so every file
// operation runs at local latency with no network client and no FUSE inside
// the guest. This package (host side, in sandboxd) moves deltas between the
// guest — over silkd via the engine — and the NAS workspace (a host mount).
//
// Coordination is through the NAS itself: each writer appends journal entries
// under <ws>/.filecache/journal/ and freshens <ws>/.filecache/seq; pullers
// poll seq (an O(1) GETATTR) and fetch only the paths named in unseen entries.
// Concurrent edits resolve last-writer-wins by NAS-observed divergence; the
// peer's version is preserved as <path>.fc-conflict-<ts>, never silently lost.
package filecache

import (
	"context"
	"io"
)

const fcDir = ".filecache"

// Guest is the subset of guest operations the sync engine needs; the engine
// package implements it over silkd. All paths are absolute guest paths.
type Guest interface {
	Run(ctx context.Context, vsockSocket string, argv ...string) (string, error)
	WriteFile(ctx context.Context, vsockSocket, path string, mode uint32, data []byte) error
	ReadFile(ctx context.Context, vsockSocket, path string) ([]byte, error)
	PushTar(ctx context.Context, vsockSocket, dest string, r io.Reader) error
	Remove(ctx context.Context, vsockSocket, path string, recursive bool) error
}

// entMeta is a workspace entry's identity for change detection.
type entMeta struct {
	Kind   string `json:"kind"` // f | l
	Size   int64  `json:"size,omitempty"`
	MtimeS int64  `json:"mtime_s,omitempty"`
	Target string `json:"target,omitempty"` // symlink target
}

// journalEntry is one writer's published delta, serialized under
// <ws>/.filecache/journal/<writer>-<seq>.json.
type journalEntry struct {
	Writer string             `json:"writer"`
	Seq    uint64             `json:"seq"`
	TsNs   int64              `json:"ts_ns"`
	Puts   map[string]entMeta `json:"puts,omitempty"`
	Dels   []string           `json:"dels,omitempty"`
}
