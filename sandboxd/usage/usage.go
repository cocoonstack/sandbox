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

// Mark places a landed line in its node's journal: Seq grows from 1 in journal order, Digest is the line's SHA-256.
type Mark struct {
	Seq    int64    `json:"seq"`
	Digest [32]byte `json:"-"`
}

// Record is one landed event; Cursor orders the store.
type Record struct {
	Cursor int64  `json:"cursor"`
	Node   string `json:"node"`
	Mark
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
	// Last returns the mark of node's newest landed event; a zero Mark when none landed.
	Last(ctx context.Context, node string) (Mark, error)
	// Land appends recs in one transaction, skipping a line whose node and digest already landed and failing whole on a landed seq; cursors grow in commit order.
	Land(ctx context.Context, recs []Record) error
	// List returns the events q selects in cursor order.
	List(ctx context.Context, q Query) ([]Record, error)
}
