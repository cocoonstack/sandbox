// Package poolset holds a node's API-applied pool set and the interface a cell-wide backend keeps it behind.
package poolset

import (
	"context"
	"errors"

	"github.com/cocoonstack/sandbox/sandboxd/config"
)

// ErrUnavailable answers a load or store the shared backend cannot serve now; a retry may succeed.
var ErrUnavailable = errors.New("pool set store down")

// Set is an API-applied pool set and the hash of the config pools it overrides.
type Set struct {
	ConfigSeed string            `json:"config_seed"`
	Pools      []config.PoolSpec `json:"pools"`
}

// Shared keeps one cell's pool set where every node of the cell reads it.
type Shared interface {
	// Load returns the cell's set and its version; a nil set means none was stored.
	Load(ctx context.Context) (*Set, int64, error)
	// Store replaces the cell's set, tells every node, and returns the new version.
	Store(ctx context.Context, set Set) (int64, error)
	// Watch calls changed whenever the set may have changed, until ctx ends.
	Watch(ctx context.Context, changed func())
	Close() error
}
