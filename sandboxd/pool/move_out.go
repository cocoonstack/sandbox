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
	mv := &outMove{Addr: addr}
	if err := m.markMoving(sb, mv); err != nil {
		return MoveResult{}, err
	}
	ctx = context.WithoutCancel(ctx)
	err := m.moveOut(ctx, sb, mv, cmp.Or(maxDowntime, defaultMoveDowntime))
	if err != nil {
		return m.failMoveOut(ctx, sb, mv, err)
	}
	result := mv.result()
	m.finishMoveOut(ctx, sb, mv)
	return result, nil
}

// On any error the caller settles the move through its record.
func (m *Manager) moveOut(ctx context.Context, sb *types.Sandbox, mv *outMove, budget time.Duration) error {
	m.mu.Lock()
	admission, vm := MoveAdmission{Tenant: sb.Tenant, Key: sb.Key, From: m.nodeID}, sb.VMName
	m.mu.Unlock()
	to, err := m.movePeer.Admit(ctx, mv.Addr, sb.ID, admission)
	if err != nil {
		return err
	}
	mv.To = to
	epoch, err := m.beginMove(ctx, sb, to)
	if errors.Is(err, moves.ErrLost) {
		return fmt.Errorf("%w: another move of %s is on record", ErrNotMovable, sb.ID)
	}
	if err != nil {
		return err
	}
	mv.Epoch = epoch
	if err = m.journalMoveOut(ctx, sb, mv); err != nil {
		return err
	}
	round, err := m.eng.MigrateCapture(ctx, vm, "", false)
	if err != nil {
		return fmt.Errorf("%w: baseline capture: %w", ErrNotMovable, err)
	}
	mv.Series = round.Series
	if err = m.journalMoveOut(ctx, sb, mv); err != nil {
		return err
	}
	var size, sent, previous int64
	var sending time.Duration
	for delta := 0; ; delta++ {
		start := time.Now()
		if size, err = m.sendRound(ctx, sb.ID, mv, round); err != nil {
			return err
		}
		sending += time.Since(start)
		sent += size
		if delta > 0 {
			if finalPause(size, sent, sending) <= budget {
				break
			}
			if delta == moveDeltaLimit || delta > 1 && size*10 >= previous*9 {
				return fmt.Errorf("%w: delta %d of %d bytes does not fit %s", ErrMoveDowntime, delta, size, budget)
			}
		}
		previous = size
		if round, err = m.eng.MigrateCapture(ctx, vm, mv.Series, false); err != nil {
			return fmt.Errorf("%w: delta capture: %w", ErrNotMovable, err)
		}
	}
	lastBytes := roundBytes(round)
	m.mu.Lock()
	mv.final = true
	m.mu.Unlock()
	sb.Transition.Lock()
	mv.locked = true
	ctx, cancel := context.WithTimeout(ctx, moveFinalTimeout)
	defer cancel()
	if err = m.unrowForMove(ctx, sb, mv); err != nil {
		return err
	}
	mv.pausedAt = time.Now()
	if round, err = m.eng.MigrateCapture(ctx, vm, mv.Series, true); err != nil {
		return fmt.Errorf("%w: final capture: %w", ErrNotMovable, err)
	}
	mv.capture = time.Since(mv.pausedAt)
	apparent := roundBytes(round)
	wire := size + max(apparent-lastBytes, 0)
	if pause := mv.capture + finalPause(wire, sent, sending); pause > budget {
		return fmt.Errorf("%w: the final round of %d bytes took %s to capture, leaving a pause of about %s", ErrMoveDowntime, apparent, mv.capture.Round(time.Millisecond), pause.Round(time.Millisecond))
	}
	final, err := m.moves.Advance(ctx, sb.ID, mv.Epoch, moves.Pending, moves.Final)
	if err != nil {
		return err
	}
	start := time.Now()
	if _, err = m.sendRound(ctx, sb.ID, mv, round); err != nil {
		return err
	}
	mv.send = time.Since(start)
	if used, ok := m.out.TunUsed(sb.ID); ok {
		m.setTunBytes(sb, used)
	}
	m.mu.Lock()
	dto := dtoOf(sb)
	m.mu.Unlock()
	claim, err := json.Marshal(&dto)
	if err != nil {
		return fmt.Errorf("encode claim: %w", err)
	}
	start = time.Now()
	err = m.movePeer.Commit(ctx, mv.Addr, sb.ID, claim, final)
	mv.commit = time.Since(start)
	return err
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
	settled := m.claimed[sb.ID] == sb && !sb.MoveIn && sb.MoveOut == nil
	m.mu.Unlock()
	if !ok || rec.State != moves.Committed || !settled {
		return 0, moves.ErrLost
	}
	if _, err = m.moves.Advance(ctx, sb.ID, rec.Epoch, moves.Committed, moves.Started); err != nil && !errors.Is(err, moves.ErrLost) {
		return 0, err
	}
	return m.moves.Begin(ctx, sb.ID, m.nodeID, to)
}

func (m *Manager) sendRound(ctx context.Context, id string, mv *outMove, round types.MigrateRound) (int64, error) {
	mv.rounds++
	names := make([]string, len(round.Files))
	for i, f := range round.Files {
		names[i] = f.Name
	}
	ctx, cancel := context.WithTimeout(ctx, moveRoundBase+time.Duration(roundBytes(round)/moveRoundMinRate)*time.Second)
	defer cancel()
	return m.movePeer.SendRound(ctx, mv.Addr, id, mv.rounds, round.Dir, names)
}

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

// Callers hold m.mu.
func (m *Manager) moveBlocker(sb *types.Sandbox) string {
	_, archiving := m.archiving[sb.ID]
	switch {
	case sb.ArchiveCk != "" || archiving:
		return "it is archived or archiving"
	case sb.HibernateSnap != "" || sb.PendingSnap != "":
		return "it is hibernated"
	case sb.SaveSnap != "":
		return "a capture of it is settling"
	case sb.Failed != "":
		return "it failed"
	case sb.Waking:
		return "it is waking"
	case m.midMoveLocked(sb):
		return "a move of it is already running"
	case m.hasVolumeHolds(sb):
		return "it has volumes"
	case sb.Key.Net != types.NetNone:
		return "an egress-lane claim does not move"
	}
	return ""
}

func (m *Manager) journalMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove) error {
	live, err := m.commitIfLive(ctx, sb, func() { sb.MoveOut = new(mv.MoveOut) })
	if !live {
		return ErrUnknownSandbox
	}
	if err != nil {
		return fmt.Errorf("journal the move of %s: %w", sb.ID, err)
	}
	return nil
}

// Without the row no third node adopts a stale copy; the caller holds sb.Transition.
func (m *Manager) unrowForMove(ctx context.Context, sb *types.Sandbox, mv *outMove) error {
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
	consumed, err := m.consumeRow(ctx, sb, epoch, "move")
	if err != nil {
		return err
	}
	live, err := m.commitIfLive(ctx, sb, func() { sb.ReplicaCk, sb.ArchiveEpoch = "", 0 })
	if !live {
		return ErrUnknownSandbox
	}
	if err != nil {
		return err
	}
	if err = m.archives.Delete(ctx, sb.ID, consumed); err != nil && !errors.Is(err, archives.ErrLost) {
		return err
	}
	mv.replica = replica
	return nil
}

// Deletes the record only once the target marked it started.
func (m *Manager) finishMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove) {
	m.mu.Lock()
	vm := sb.VMName
	m.mu.Unlock()
	if teardown := m.forgetAdopted(ctx, sb); teardown != nil {
		teardown()
		m.recordUsage(ctx, usage.Event{Event: "move_out", ID: sb.ID, VMName: vm, Tenant: sb.Tenant})
	}
	m.endMoveOut(ctx, sb, mv)
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

// Only proof the move stayed here resumes the guest; anything else drops the claim.
func (m *Manager) failMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove, cause error) (MoveResult, error) {
	if mv.Epoch == 0 {
		_ = m.abortMoveOut(ctx, sb, mv, 0)
		return MoveResult{}, cause
	}
	result := mv.result()
	moved, err := m.settleMoveOut(ctx, sb, mv)
	switch {
	case err != nil:
		log.WithFunc("pool.failMoveOut").Warnf(ctx, "move of %s failed (%v) and its abort is retrying: %v", sb.ID, cause, err)
		go m.retryMoveOut(ctx, sb, mv)
		return MoveResult{}, errors.Join(cause, err)
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

// An error leaves the move as it is.
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

// Deletes the aborted record only once the journal no longer needs it.
func (m *Manager) abortMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove, aborted int64) error {
	logger := log.WithFunc("pool.abortMoveOut")
	m.mu.Lock()
	vm, live := sb.VMName, m.claimed[sb.ID] == sb
	m.mu.Unlock()
	if live && mv.Series != "" {
		if err := m.eng.MigrateAbort(ctx, vm, mv.Series); err != nil {
			return fmt.Errorf("abort series %s: %w", mv.Series, err)
		}
	}
	if mv.Addr != "" && mv.To != "" {
		if err := m.movePeer.Drop(ctx, mv.Addr, sb.ID); err != nil {
			logger.Warnf(ctx, "drop the reservation for %s on %s: %v", sb.ID, mv.Addr, err)
		}
	}
	_, journalErr := m.commitIfLive(ctx, sb, func() { sb.MoveOut = nil })
	m.endMoveOut(ctx, sb, mv)
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

func (m *Manager) endMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove) {
	m.mu.Lock()
	if m.moving[sb.ID] == mv {
		delete(m.moving, sb.ID)
	}
	m.mu.Unlock()
	if mv.locked {
		mv.locked = false
		sb.Transition.Unlock()
	}
	if mv.replica != "" {
		m.deleteOrphanArchiveCk(ctx, mv.replica)
	}
}

func (m *Manager) pausedForMove(sb *types.Sandbox) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pausedForMoveLocked(sb)
}

// Paused: a source's final round, or a received claim not yet started.
func (m *Manager) pausedForMoveLocked(sb *types.Sandbox) bool {
	mv := m.moving[sb.ID]
	return sb.MoveIn || mv != nil && mv.final
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
	return moving || sb.MoveIn
}

func roundBytes(round types.MigrateRound) int64 {
	var size int64
	for _, f := range round.Files {
		size += f.Size
	}
	return size
}

func finalPause(last, sent int64, sending time.Duration) time.Duration {
	return MoveStartAllowance + time.Duration(float64(sending)*float64(last)/float64(sent))
}
