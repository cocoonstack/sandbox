// Package usage defines the usage journal's events.
package usage

import "time"

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
