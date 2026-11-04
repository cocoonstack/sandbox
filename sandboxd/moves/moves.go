// Package moves keeps one cluster-wide record per live move of a running claim from a source node to a target node.
package moves

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	Pending   State = "pending"
	Final     State = "final"
	Committed State = "committed"
	Started   State = "started"
	Aborted   State = "aborted"
)

var (
	// ErrUnavailable answers a call the registry cannot serve now; a retry may succeed.
	ErrUnavailable = errors.New("move registry down")
	// ErrLost answers a compare-and-set whose record is gone, at another epoch, or in another state.
	ErrLost = errors.New("move record moved on")

	advances = map[[2]State]bool{
		{Pending, Final}:     true,
		{Final, Committed}:   true,
		{Committed, Started}: true,
		{Pending, Aborted}:   true,
		{Final, Aborted}:     true,
	}
)

// State is where a move stands; Started and Aborted end it.
type State string

// Record is one claim's move.
type Record struct {
	ID        string
	From      string
	To        string
	State     State
	Epoch     int64
	UpdatedAt time.Time
}

// Registry keeps the records; every write is a compare-and-set, and every epoch it hands out is new.
type Registry interface {
	// Begin writes a Pending record for id at a new epoch and returns it; ErrLost when a Pending, Final or Committed record holds id.
	Begin(ctx context.Context, id, from, to string) (int64, error)
	// Advance moves the record at epoch expect in state from to state to at a new epoch and returns it.
	Advance(ctx context.Context, id string, expect int64, from, to State) (int64, error)
	// Get returns the record of id; false when there is none.
	Get(ctx context.Context, id string) (Record, bool, error)
	// Delete removes the record only while it is at epoch.
	Delete(ctx context.Context, id string, epoch int64) error
	// Stale returns the records last written before before.
	Stale(ctx context.Context, before time.Time) ([]Record, error)
}

// VerifyAdvance refuses a state change no move makes.
func VerifyAdvance(from, to State) error {
	if !advances[[2]State{from, to}] {
		return fmt.Errorf("move state %s cannot advance to %s", from, to)
	}
	return nil
}
