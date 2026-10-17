// Package archives keeps one cluster-wide row per archived claim, so another node can adopt the claim once its owner is gone.
package archives

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"time"
)

var (
	// ErrUnavailable answers a call the registry cannot serve now; a retry may succeed.
	ErrUnavailable = errors.New("archive registry down")
	// ErrLost answers a compare-and-set whose row is gone, at another epoch, consumed, or held by another owner.
	ErrLost = errors.New("archive row moved on")
)

// Record is one archived claim's row.
type Record struct {
	ID          string
	Tenant      string
	TokenSHA256 string
	Owner       string
	Epoch       int64
	Consumed    bool
	Released    bool
	Checkpoint  string
	// Deadline is the retention deadline; zero keeps the archive forever.
	Deadline  time.Time
	UpdatedAt time.Time
	Claim     jsontext.Value
}

// Registry keeps the rows; every write is a compare-and-set, and every epoch it hands out is new.
type Registry interface {
	// Put writes rec for rec.Owner as a live row at a new epoch and returns it; ErrLost when another owner holds the row.
	Put(ctx context.Context, rec Record) (int64, error)
	// Take moves the live row at epoch expect to owner at a new epoch and returns it.
	Take(ctx context.Context, id string, expect int64, owner string) (int64, error)
	// Consume turns the live row at epoch expect into a tombstone at a new epoch, marked released or woken, and returns it.
	Consume(ctx context.Context, id string, expect int64, released bool) (int64, error)
	// Revive turns the woken tombstone at epoch expect back into a live row at a new epoch and returns it.
	Revive(ctx context.Context, id string, expect int64) (int64, error)
	// Delete removes the row only while it is at epoch.
	Delete(ctx context.Context, id string, epoch int64) error
	// Get returns the row of id; false when there is none.
	Get(ctx context.Context, id string) (Record, bool, error)
	// Sweepable returns the rows past their deadline, consumed, or held by owner, without their claims.
	Sweepable(ctx context.Context, now time.Time, owner string) ([]Record, error)
	// Rows returns the rows of ids by id, without their claims.
	Rows(ctx context.Context, ids []string) (map[string]Record, error)
}
