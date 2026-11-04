package pool

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/moves"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

// Move hands a running claim to the peer at addr with cocoon's live migration, pausing its guest for about maxDowntime at most.
func (m *Manager) Move(ctx context.Context, id, addr string, maxDowntime time.Duration) (MoveResult, error) {
	sb, ok := m.byID(id)
	if !ok {
		return MoveResult{}, ErrUnknownSandbox
	}
	if m.moves == nil || m.movePeer == nil {
		return MoveResult{}, errMovesOff
	}
	if _, err := m.fenceWatch(sb); err != nil {
		return MoveResult{}, err
	}
	if err := m.eng.CheckMigrateSupport(ctx); err != nil {
		return MoveResult{}, fmt.Errorf("%w: %w", ErrNotMovable, err)
	}
	mv := &outMove{addr: addr}
	if err := m.markMoving(sb, mv); err != nil {
		return MoveResult{}, err
	}
	ctx = context.WithoutCancel(ctx)
	err := m.moveOut(ctx, sb, mv, cmp.Or(maxDowntime, DefaultMoveDowntime))
	if err != nil {
		return m.failMoveOut(ctx, sb, mv, err)
	}
	result := mv.result()
	m.finishMoveOut(ctx, sb, mv)
	return result, nil
}

// moveOut runs the rounds up to the target's commit; on any error the caller resolves the move through its record.
func (m *Manager) moveOut(ctx context.Context, sb *types.Sandbox, mv *outMove, budget time.Duration) error {
	m.mu.Lock()
	admission, vm := MoveAdmission{Tenant: sb.Tenant, Key: sb.Key, From: m.nodeID}, sb.VMName
	m.mu.Unlock()
	to, err := m.movePeer.Admit(ctx, mv.addr, sb.ID, admission)
	if err != nil {
		return err
	}
	mv.to = to
	epoch, err := m.beginMove(ctx, sb, to)
	if errors.Is(err, moves.ErrLost) {
		return fmt.Errorf("%w: another move of %s is on record", ErrNotMovable, sb.ID)
	}
	if err != nil {
		return err
	}
	mv.epoch = epoch
	if err = m.journalMoveOut(ctx, sb, mv); err != nil {
		return err
	}
	round, err := m.eng.MigrateCapture(ctx, vm, "", false)
	if err != nil {
		return fmt.Errorf("%w: baseline capture: %w", ErrNotMovable, err)
	}
	mv.series = round.Series
	if err = m.journalMoveOut(ctx, sb, mv); err != nil {
		return err
	}
	var sent, previous int64
	var sending time.Duration
	for delta := 0; ; delta++ {
		start := time.Now()
		if err = m.sendRound(ctx, sb.ID, mv, round); err != nil {
			return err
		}
		sending += time.Since(start)
		size := roundBytes(round)
		sent += size
		if delta > 0 {
			if finalPause(size, sent, sending) <= budget {
				break
			}
			if delta == moveDeltaLimit || delta > 1 && size >= previous {
				return fmt.Errorf("%w: delta %d of %d bytes does not fit %s", ErrMoveDowntime, delta, size, budget)
			}
		}
		previous = size
		if round, err = m.eng.MigrateCapture(ctx, vm, mv.series, false); err != nil {
			return fmt.Errorf("delta capture: %w", err)
		}
	}
	m.mu.Lock()
	mv.final = true
	m.mu.Unlock()
	sb.Transition.Lock()
	mv.locked = true
	ctx, cancel := context.WithTimeout(ctx, moveFinalTimeout)
	defer cancel()
	if err = m.unrowForMove(ctx, sb); err != nil {
		return err
	}
	mv.pausedAt = time.Now()
	if round, err = m.eng.MigrateCapture(ctx, vm, mv.series, true); err != nil {
		return fmt.Errorf("final capture: %w", err)
	}
	final, err := m.moves.Advance(ctx, sb.ID, mv.epoch, moves.Pending, moves.Final)
	if err != nil {
		return err
	}
	if err = m.sendRound(ctx, sb.ID, mv, round); err != nil {
		return err
	}
	m.mu.Lock()
	dto := dtoOf(sb)
	m.mu.Unlock()
	dto.MoveOut = nil
	claim, err := json.Marshal(&dto)
	if err != nil {
		return fmt.Errorf("encode claim: %w", err)
	}
	return m.movePeer.Commit(ctx, mv.addr, sb.ID, claim, final)
}

func (m *Manager) beginMove(ctx context.Context, sb *types.Sandbox, to string) (int64, error) {
	epoch, err := m.moves.Begin(ctx, sb.ID, m.nodeID, to)
	if !errors.Is(err, moves.ErrLost) {
		return epoch, err
	}
	rec, ok, err := m.moves.Get(ctx, sb.ID)
	if err != nil {
		return 0, err
	}
	m.mu.Lock()
	settled := m.claimed[sb.ID] == sb && sb.MoveIn == nil && sb.MoveOut == nil
	m.mu.Unlock()
	if !ok || rec.State != moves.Committed || !settled {
		return 0, moves.ErrLost
	}
	if _, err = m.moves.Advance(ctx, sb.ID, rec.Epoch, moves.Committed, moves.Started); err != nil && !errors.Is(err, moves.ErrLost) {
		return 0, err
	}
	return m.moves.Begin(ctx, sb.ID, m.nodeID, to)
}

func (m *Manager) sendRound(ctx context.Context, id string, mv *outMove, round types.MigrateRound) error {
	mv.rounds++
	names := make([]string, len(round.Files))
	for i, f := range round.Files {
		names[i] = f.Name
	}
	ctx, cancel := context.WithTimeout(ctx, roundTimeout(roundBytes(round)))
	defer cancel()
	return m.movePeer.SendRound(ctx, mv.addr, id, mv.rounds, round.Dir, names)
}

// markMoving admits a move of sb and gates its captures, hibernates, releases and reaps until the move ends.
func (m *Manager) markMoving(sb *types.Sandbox, mv *outMove) error {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimed[sb.ID] != sb {
		return ErrUnknownSandbox
	}
	if reason := m.moveBlocker(sb); reason != "" {
		return fmt.Errorf("%w: %s", ErrNotMovable, reason)
	}
	switch {
	case m.evacuatingLocked(sb.ID):
		return ErrEvacuating
	case m.tenantGone(sb.Tenant):
		return ErrTenantRemoved
	}
	if err := m.moveBudgetErr(); err != nil {
		return err
	}
	m.moving[sb.ID] = mv
	return nil
}

// moveBlocker names why sb cannot move, "" when it can; callers hold m.mu.
func (m *Manager) moveBlocker(sb *types.Sandbox) string {
	_, archiving := m.archiving[sb.ID]
	switch {
	case sb.ArchiveCk != "" || archiving:
		return "it is archived or archiving"
	case sb.HibernateSnap != "" || sb.PendingSnap != "":
		return "it is hibernated"
	case sb.Failed != "":
		return "it failed"
	case sb.Waking:
		return "it is waking"
	case m.midMoveLocked(sb):
		return "a move of it is already running"
	case hasVolumes(sb) || m.hasVolumeHolds(sb):
		return "it has volumes"
	case sb.Key.Net != types.NetNone:
		return "an egress-lane claim does not move"
	}
	return ""
}

func (m *Manager) journalMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove) error {
	journal := &types.MoveOut{To: mv.to, Epoch: mv.epoch, Series: mv.series}
	live, err := m.commitIfLive(ctx, sb, func() { sb.MoveOut = journal })
	if !live {
		return ErrUnknownSandbox
	}
	if err != nil {
		return fmt.Errorf("journal the move of %s: %w", sb.ID, err)
	}
	return nil
}

// unrowForMove deletes a replicated claim's row and replica, so no third node adopts a stale copy while the target runs it; the caller holds sb.Transition.
func (m *Manager) unrowForMove(ctx context.Context, sb *types.Sandbox) error {
	if m.archives == nil {
		return nil
	}
	if err := m.settleReplicaIntent(ctx, sb); err != nil {
		return err
	}
	m.mu.Lock()
	replica, epoch := sb.ReplicaCk, sb.ArchiveEpoch
	m.mu.Unlock()
	if epoch == 0 {
		return nil
	}
	if err := m.archives.Delete(ctx, sb.ID, epoch); errors.Is(err, archives.ErrLost) {
		return fmt.Errorf("%w: its replica row moved under it", ErrNotMovable)
	} else if err != nil {
		return err
	}
	live, err := m.commitIfLive(ctx, sb, func() { sb.ReplicaCk, sb.ArchiveEpoch = "", 0 })
	if !live {
		return ErrUnknownSandbox
	}
	if replica != "" {
		m.deleteOrphanArchiveCk(ctx, replica)
	}
	return err
}

// finishMoveOut drops a claim that moved on, like a claim another node adopted, and deletes the record once its target marked it started.
func (m *Manager) finishMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove) {
	m.mu.Lock()
	vm := sb.VMName
	m.mu.Unlock()
	if teardown := m.forgetAdopted(ctx, sb); teardown != nil {
		teardown()
		m.recordUsage(ctx, usage.Event{Event: "move_out", ID: sb.ID, VMName: vm, Tenant: sb.Tenant})
	}
	m.endMoveOut(sb, mv)
	logger := log.WithFunc("pool.finishMoveOut")
	rec, ok, err := m.moves.Get(ctx, sb.ID)
	if err != nil {
		logger.Warnf(ctx, "read the move of %s, leaving it to the sweep: %v", sb.ID, err)
		return
	}
	if ok && mv.owns(rec, m.nodeID) && rec.State == moves.Started {
		if err = m.moves.Delete(ctx, sb.ID, rec.Epoch); err != nil {
			logger.Warnf(ctx, "delete the move of %s, leaving it to the sweep: %v", sb.ID, err)
		}
	}
}

// failMoveOut resolves a failed move through its record: only proof that the move stayed here resumes the guest, anything else drops the claim.
func (m *Manager) failMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove, cause error) (MoveResult, error) {
	if mv.epoch == 0 {
		_ = m.abortMoveOut(ctx, sb, mv, 0)
		return MoveResult{}, cause
	}
	result := mv.result()
	moved, err := m.settleMoveOut(ctx, sb, mv)
	switch {
	case err != nil:
		log.WithFunc("pool.failMoveOut").Warnf(ctx, "move of %s failed (%v) and its abort waits for the registry: %v", sb.ID, cause, err)
		go m.retryMoveOut(ctx, sb, mv)
		return MoveResult{}, fmt.Errorf("%w: %w", ErrMoveStoreDown, cause)
	case moved:
		return result, nil
	}
	return MoveResult{}, cause
}

func (m *Manager) retryMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove) {
	for {
		_, err := m.settleMoveOut(ctx, sb, mv)
		if err == nil {
			return
		}
		log.WithFunc("pool.retryMoveOut").Warnf(ctx, "abort the move of %s, retrying: %v", sb.ID, err)
		time.Sleep(moveRetryInterval)
	}
}

// settleMoveOut resumes the guest only on a record of this move still open or aborted; a missing record, another node's, or a committed or started one means the claim moved on. An error leaves the move as it is.
func (m *Manager) settleMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove) (bool, error) {
	for range 2 {
		rec, ok, err := m.moves.Get(ctx, sb.ID)
		if err != nil {
			return false, err
		}
		ours := ok && mv.owns(rec, m.nodeID)
		switch {
		case !ours || rec.State == moves.Committed || rec.State == moves.Started:
			m.finishMoveOut(ctx, sb, mv)
			return true, nil
		case rec.State == moves.Pending || rec.State == moves.Final:
			rec.Epoch, err = m.moves.Advance(ctx, sb.ID, rec.Epoch, rec.State, moves.Aborted)
			if errors.Is(err, moves.ErrLost) {
				continue
			}
			if err != nil {
				return false, err
			}
		}
		return false, m.abortMoveOut(ctx, sb, mv, rec.Epoch)
	}
	return false, fmt.Errorf("the move record of %s kept changing", sb.ID)
}

// abortMoveOut ends the series, which resumes a paused guest, releases the target and the budget slot, and deletes the aborted record only once the journal no longer needs it.
func (m *Manager) abortMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove, aborted int64) error {
	logger := log.WithFunc("pool.abortMoveOut")
	m.mu.Lock()
	vm, live := sb.VMName, m.claimed[sb.ID] == sb
	m.mu.Unlock()
	if live && mv.series != "" {
		if err := m.eng.MigrateAbort(ctx, vm, mv.series); err != nil {
			return fmt.Errorf("abort series %s: %w", mv.series, err)
		}
	}
	if mv.addr != "" && mv.to != "" {
		if err := m.movePeer.Drop(ctx, mv.addr, sb.ID); err != nil {
			logger.Warnf(ctx, "drop the reservation for %s on %s: %v", sb.ID, mv.addr, err)
		}
	}
	_, journalErr := m.commitIfLive(ctx, sb, func() { sb.MoveOut = nil })
	m.endMoveOut(sb, mv)
	if journalErr != nil {
		logger.Warnf(ctx, "clear the move journal of %s, leaving its record to the sweep: %v", sb.ID, journalErr)
		return nil
	}
	if aborted != 0 {
		if err := m.moves.Delete(ctx, sb.ID, aborted); err != nil {
			logger.Warnf(ctx, "delete the aborted move of %s, leaving it to the sweep: %v", sb.ID, err)
		}
	}
	return nil
}

func (m *Manager) endMoveOut(sb *types.Sandbox, mv *outMove) {
	m.mu.Lock()
	if m.moving[sb.ID] == mv {
		delete(m.moving, sb.ID)
	}
	m.mu.Unlock()
	if mv.locked {
		mv.locked = false
		sb.Transition.Unlock()
	}
}

// pausedForMove reports whether sb's guest is paused by a move, in a source's final round or received and not started, when dials and wakes answer ErrEvacuating.
func (m *Manager) pausedForMove(sb *types.Sandbox) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pausedForMoveLocked(sb)
}

func (m *Manager) pausedForMoveLocked(sb *types.Sandbox) bool {
	mv := m.moving[sb.ID]
	return sb.MoveIn != nil || mv != nil && mv.final
}

func (m *Manager) lockTransition(sb *types.Sandbox) error {
	midMove := func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.midMoveLocked(sb)
	}
	if midMove() {
		return ErrEvacuating
	}
	sb.Transition.Lock()
	if midMove() {
		sb.Transition.Unlock()
		return ErrEvacuating
	}
	return nil
}

func (m *Manager) midMoveLocked(sb *types.Sandbox) bool {
	_, moving := m.moving[sb.ID]
	return moving || sb.MoveIn != nil
}

func roundBytes(round types.MigrateRound) int64 {
	var n int64
	for _, f := range round.Files {
		n += f.Size
	}
	return n
}

func roundTimeout(size int64) time.Duration {
	return moveRoundBase + time.Duration(size/moveRoundMinRate)*time.Second
}

// finalPause estimates the final round's pause from the last delta's size at the rate the rounds so far reached.
func finalPause(last, sent int64, sending time.Duration) time.Duration {
	if sent <= 0 {
		return MoveStartAllowance
	}
	return MoveStartAllowance + time.Duration(float64(sending)*float64(last)/float64(sent))
}
