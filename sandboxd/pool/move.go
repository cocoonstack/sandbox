package pool

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/moves"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	// DefaultMoveDowntime is the final round's pause a move allows when the caller names none.
	DefaultMoveDowntime = 2 * time.Second
	// MoveStartAllowance is what the target's receive and start add to the final round's transfer, so a smaller downtime budget never fits.
	MoveStartAllowance = 400 * time.Millisecond

	movesDir = "moves"
	// A reservation with no round or commit for this long belongs to a source that gave up.
	moveReservationIdle = 2 * time.Minute
	moveRetryInterval   = 2 * time.Second
	moveDeltaLimit      = 4
	moveStaleAge        = time.Hour
	moveFinalTimeout    = 2 * time.Minute
	moveRoundBase       = time.Minute
	moveRoundMinRate    = 10_000_000
)

// MoveAdmission is what a source asks a target to reserve for a live move.
type MoveAdmission struct {
	Tenant string
	Key    types.PoolKey
	From   string
}

// MoveResult reports a finished live move: the rounds sent and the guest's pause from the final capture to the target's commit.
type MoveResult struct {
	Rounds   int
	Downtime time.Duration
}

// MovePeer reaches the target node of a live move.
type MovePeer interface {
	// Admit reserves a claim slot on the target and returns its node id.
	Admit(ctx context.Context, addr, id string, a MoveAdmission) (string, error)
	SendRound(ctx context.Context, addr, id string, round int, dir string, files []string) error
	// Commit hands the target the claim record and the final epoch.
	Commit(ctx context.Context, addr, id string, claim []byte, epoch int64) error
	Drop(ctx context.Context, addr, id string) error
}

// outMove is a source's move in flight; final is guarded by m.mu, the rest belongs to the goroutine driving the move.
type outMove struct {
	types.MoveOut
	addr     string
	to       string
	replica  string
	rounds   int
	final    bool
	locked   bool
	pausedAt time.Time
}

func (mv *outMove) owns(rec moves.Record, node string) bool {
	return rec.From == node && rec.Epoch >= mv.Epoch
}

func (mv *outMove) result() MoveResult {
	res := MoveResult{Rounds: mv.rounds}
	if !mv.pausedAt.IsZero() {
		res.Downtime = time.Since(mv.pausedAt)
	}
	return res
}

// WithMovePeer wires the transport a move uses to reach its target; set it before serving.
func (m *Manager) WithMovePeer(p MovePeer) {
	m.movePeer = p
}

// moveBudgetErr refuses a move past move_concurrency, counting both directions; callers hold m.mu.
func (m *Manager) moveBudgetErr() error {
	limit := m.view.Load().moveConcurrency
	if n := len(m.moving) + len(m.incoming); n >= limit {
		return fmt.Errorf("%w: %d moves in flight, move_concurrency %d", ErrQuota, n, limit)
	}
	return nil
}

func (m *Manager) moveDir(id string) string {
	return filepath.Join(m.dataDir, movesDir, id)
}

// sweepStaleMoves ends this node's stale moves no move in flight holds: a target marks a committed one started, either side aborts an open one, and only the source deletes an ended one.
func (m *Manager) sweepStaleMoves(ctx context.Context, now time.Time) {
	logger := log.WithFunc("pool.sweepStaleMoves")
	recs, err := m.moves.Stale(ctx, now.Add(-moveStaleAge))
	if err != nil {
		logger.Warnf(ctx, "list stale moves: %v", err)
		return
	}
	for _, rec := range recs {
		if rec.From != m.nodeID && rec.To != m.nodeID || m.moveHeld(rec.ID) {
			continue
		}
		epoch := rec.Epoch
		switch rec.State {
		case moves.Committed:
			if rec.To == m.nodeID {
				m.markMoveStarted(ctx, rec.ID, rec.Epoch)
			}
			continue
		case moves.Pending, moves.Final:
			if epoch, err = m.moves.Advance(ctx, rec.ID, rec.Epoch, rec.State, moves.Aborted); err != nil {
				logger.Warnf(ctx, "abort stale move %s: %v", rec.ID, err)
				continue
			}
		}
		if rec.From != m.nodeID {
			continue
		}
		if err = m.moves.Delete(ctx, rec.ID, epoch); err != nil {
			logger.Warnf(ctx, "delete stale move %s: %v", rec.ID, err)
		}
	}
}

func (m *Manager) moveHeld(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, out := m.moving[id]
	_, in := m.incoming[id]
	sb := m.claimed[id]
	return out || in || sb != nil && sb.MoveIn
}
