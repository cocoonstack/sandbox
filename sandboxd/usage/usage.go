// Package usage defines the usage journal's events and lands a node's journal in a store the whole cell shares.
package usage

import (
	"context"
	"time"
)

// JournalFile is the usage journal's name under data_dir; it rotates to JournalFile+".1".
const JournalFile = "usage.jsonl"

// Event is one line of usage.jsonl, the billing stream the platform collector folds.
type Event struct {
	Time      time.Time `json:"t"`
	Event     string    `json:"ev"`
	ID        string    `json:"id"`
	VMName    string    `json:"vm,omitempty"`
	KeyHash   string    `json:"key,omitempty"`
	Tenant    string    `json:"tenant,omitempty"`
	Volumes   []string  `json:"volumes,omitempty"`
	VolumesRW []string  `json:"volumes_rw,omitempty"`
	Children  []string  `json:"children,omitempty"`
	Reference string    `json:"ref,omitempty"`
	Upstream  string    `json:"upstream,omitempty"`
	Secret    string    `json:"secret,omitempty"`
	Sent      int64     `json:"tx,omitzero"`
	Received  int64     `json:"rx,omitzero"`
}

// Record is one landed event; Cursor orders the store and Digest is the journal line's SHA-256.
type Record struct {
	Cursor int64    `json:"cursor"`
	Node   string   `json:"node"`
	Digest [32]byte `json:"-"`
	Event
}

// Query selects events with a cursor above After, of Tenant when set, timed in [Since, Until) where set, at most Limit.
type Query struct {
	Tenant string
	Since  time.Time
	Until  time.Time
	After  int64
	Limit  int
}

// Store keeps the landed events of every node of a cell.
type Store interface {
	// Last returns the digest of node's newest landed line; zero when none landed.
	Last(ctx context.Context, node string) ([32]byte, error)
	// Land appends recs in one transaction, skipping a line whose node and digest already landed; cursors grow in commit order.
	Land(ctx context.Context, recs []Record) error
	// List returns the events q selects in cursor order.
	List(ctx context.Context, q Query) ([]Record, error)
}
