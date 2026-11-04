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

type movedAt struct {
	epoch    int64
	downtime time.Duration
}

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
	paused, err := m.moveOut(ctx, sb, mv, cmp.Or(maxDowntime, DefaultMoveDowntime))
	if err == nil {
		m.finishMoveOut(ctx, sb, mv, paused.epoch)
		return MoveResult{Rounds: mv.rounds, Downtime: paused.downtime}, nil
	}
	return m.failMoveOut(ctx, sb, mv, err)
}

// moveOut runs the rounds up to the target's commit; on any error the caller resolves the move through its record.
func (m *Manager) moveOut(ctx context.Context, sb *types.Sandbox, mv *outMove, budget time.Duration) (movedAt, error) {
	m.mu.Lock()
	admission, vm := MoveAdmission{Tenant: sb.Tenant, Key: sb.Key, From: m.nodeID}, sb.VMName
	m.mu.Unlock()
	to, err := m.movePeer.Admit(ctx, mv.addr, sb.ID, admission)
	if err != nil {
		return movedAt{}, err
	}
	mv.to = to
	epoch, err := m.moves.Begin(ctx, sb.ID, m.nodeID, to)
	if errors.Is(err, moves.ErrLost) {
		return movedAt{}, fmt.Errorf("%w: another move of %s is on record", ErrNotMovable, sb.ID)
	}
	if err != nil {
		return movedAt{}, err
	}
	mv.epoch = epoch
	if err = m.journalMoveOut(ctx, sb, mv); err != nil {
		return movedAt{}, err
	}
	round, err := m.eng.MigrateCapture(ctx, vm, "", false)
	if err != nil {
		return movedAt{}, fmt.Errorf("%w: baseline capture: %w", ErrNotMovable, err)
	}
	mv.series = round.Series
	if err = m.journalMoveOut(ctx, sb, mv); err != nil {
		return movedAt{}, err
	}
	var sent int64
	var sending time.Duration
	fits := false
	for i := 0; ; i++ {
		start := time.Now()
		if err = m.sendRound(ctx, sb.ID, mv, round); err != nil {
			return movedAt{}, err
		}
		sending += time.Since(start)
		size := roundBytes(round)
		sent += size
		if i > 0 && finalPause(size, sent, sending) <= budget {
			fits = true
		}
		if fits || i == moveDeltaLimit {
			break
		}
		if round, err = m.eng.MigrateCapture(ctx, vm, mv.series, false); err != nil {
			return movedAt{}, fmt.Errorf("delta capture: %w", err)
		}
	}
	if !fits {
		return movedAt{}, fmt.Errorf("%w: %d deltas did not shrink under %s", ErrMoveDowntime, moveDeltaLimit, budget)
	}
	m.mu.Lock()
	mv.final = true
	m.mu.Unlock()
	sb.Transition.Lock()
	mv.locked = true
	if err = m.unrowForMove(ctx, sb); err != nil {
		return movedAt{}, err
	}
	pausedAt := time.Now()
	if round, err = m.eng.MigrateCapture(ctx, vm, mv.series, true); err != nil {
		return movedAt{}, fmt.Errorf("final capture: %w", err)
	}
	final, err := m.moves.Advance(ctx, sb.ID, mv.epoch, moves.Pending, moves.Final)
	if err != nil {
		return movedAt{}, err
	}
	if err = m.sendRound(ctx, sb.ID, mv, round); err != nil {
		return movedAt{}, err
	}
	m.mu.Lock()
	dto := dtoOf(sb)
	m.mu.Unlock()
	dto.MoveOut = nil
	claim, err := json.Marshal(&dto)
	if err != nil {
		return movedAt{}, fmt.Errorf("encode claim: %w", err)
	}
	committed, err := m.movePeer.Commit(ctx, mv.addr, sb.ID, claim, final)
	if err != nil {
		return movedAt{}, err
	}
	return movedAt{epoch: committed, downtime: time.Since(pausedAt)}, nil
}

func (m *Manager) sendRound(ctx context.Context, id string, mv *outMove, round types.MigrateRound) error {
	mv.rounds++
	names := make([]string, len(round.Files))
	for i, f := range round.Files {
		names[i] = f.Name
	}
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
	_, moving := m.moving[sb.ID]
	switch {
	case sb.ArchiveCk != "" || archiving:
		return "it is archived or archiving"
	case sb.HibernateSnap != "" || sb.PendingSnap != "":
		return "it is hibernated"
	case sb.Failed != "":
		return "it failed"
	case sb.Waking:
		return "it is waking"
	case sb.MoveIn != nil || moving:
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

// finishMoveOut drops a claim the target committed, like a claim another node adopted.
func (m *Manager) finishMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove, committed int64) {
	m.mu.Lock()
	vm := sb.VMName
	m.mu.Unlock()
	if teardown := m.forgetAdopted(ctx, sb); teardown != nil {
		teardown()
		m.recordUsage(ctx, usage.Event{Event: "move_out", ID: sb.ID, VMName: vm, Tenant: sb.Tenant})
	}
	if err := m.moves.Delete(ctx, sb.ID, committed); err != nil {
		log.WithFunc("pool.finishMoveOut").Warnf(ctx, "delete the committed move of %s: %v", sb.ID, err)
	}
	m.endMoveOut(sb, mv)
}

// failMoveOut resolves a failed move through its record: committed finishes it, anything else aborts and resumes the guest.
func (m *Manager) failMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove, cause error) (MoveResult, error) {
	if mv.epoch == 0 {
		m.abortMoveOut(ctx, sb, mv)
		return MoveResult{}, cause
	}
	committed, err := m.settleMoveOut(ctx, sb, mv)
	switch {
	case err != nil:
		log.WithFunc("pool.failMoveOut").Warnf(ctx, "move of %s failed (%v) and its abort waits for the registry: %v", sb.ID, cause, err)
		go m.retryMoveOut(ctx, sb, mv)
		return MoveResult{}, fmt.Errorf("%w: %w", ErrMoveStoreDown, cause)
	case committed:
		return MoveResult{Rounds: mv.rounds}, nil
	}
	return MoveResult{}, cause
}

func (m *Manager) retryMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove) {
	for {
		time.Sleep(moveRetryInterval)
		_, err := m.settleMoveOut(ctx, sb, mv)
		if err == nil {
			return
		}
		log.WithFunc("pool.retryMoveOut").Warnf(ctx, "abort the move of %s, retrying: %v", sb.ID, err)
	}
}

// settleMoveOut reads the record: committed drops the claim, pending or final is aborted before the guest resumes; an error leaves the move as it is.
func (m *Manager) settleMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove) (bool, error) {
	for range 2 {
		rec, ok, err := m.moves.Get(ctx, sb.ID)
		if err != nil {
			return false, err
		}
		ours := ok && rec.From == m.nodeID && rec.Epoch >= mv.epoch
		switch {
		case ours && rec.State == moves.Committed:
			m.finishMoveOut(ctx, sb, mv, rec.Epoch)
			return true, nil
		case ours && (rec.State == moves.Pending || rec.State == moves.Final):
			aborted, err := m.moves.Advance(ctx, sb.ID, rec.Epoch, rec.State, moves.Aborted)
			if errors.Is(err, moves.ErrLost) {
				continue
			}
			if err != nil {
				return false, err
			}
			rec.Epoch, rec.State = aborted, moves.Aborted
		}
		if ours && rec.State == moves.Aborted {
			if err = m.moves.Delete(ctx, sb.ID, rec.Epoch); err != nil && !errors.Is(err, moves.ErrLost) {
				return false, err
			}
		}
		return false, m.abortMoveOut(ctx, sb, mv)
	}
	return false, fmt.Errorf("the move record of %s kept changing", sb.ID)
}

// abortMoveOut ends the series, which resumes a paused guest, and releases the target and the budget slot.
func (m *Manager) abortMoveOut(ctx context.Context, sb *types.Sandbox, mv *outMove) error {
	if mv.series != "" {
		m.mu.Lock()
		vm := sb.VMName
		m.mu.Unlock()
		if err := m.eng.MigrateAbort(ctx, vm, mv.series); err != nil {
			return fmt.Errorf("abort series %s: %w", mv.series, err)
		}
	}
	if mv.addr != "" && mv.to != "" {
		if err := m.movePeer.Drop(ctx, mv.addr, sb.ID); err != nil {
			log.WithFunc("pool.abortMoveOut").Warnf(ctx, "drop the reservation for %s on %s: %v", sb.ID, mv.addr, err)
		}
	}
	if _, err := m.commitIfLive(ctx, sb, func() { sb.MoveOut = nil }); err != nil {
		log.WithFunc("pool.abortMoveOut").Warnf(ctx, "clear the move journal of %s: %v", sb.ID, err)
	}
	m.endMoveOut(sb, mv)
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

// movingFinal reports whether sb's guest is in the final round of a move, when dials and wakes answer ErrEvacuating.
func (m *Manager) movingFinal(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	mv := m.moving[id]
	return mv != nil && mv.final
}

func (m *Manager) movingNow(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, moving := m.moving[id]
	return moving
}

func roundBytes(round types.MigrateRound) int64 {
	var n int64
	for _, f := range round.Files {
		n += f.Size
	}
	return n
}

// finalPause estimates the final round's pause from the last delta's size at the rate the rounds so far reached.
func finalPause(last, sent int64, sending time.Duration) time.Duration {
	if sent <= 0 {
		return moveStartAllowance
	}
	return moveStartAllowance + time.Duration(float64(sending)*float64(last)/float64(sent))
}
