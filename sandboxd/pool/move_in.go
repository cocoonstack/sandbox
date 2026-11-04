package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/moves"
	"github.com/cocoonstack/sandbox/sandboxd/store/peer"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

const reasonMoveStartFailed = "vmm did not start after a live move"

// Guarded by m.mu.
type inMove struct {
	tenant    string
	from      string
	touched   time.Time
	receiving int
}

// AdmitMove reserves a claim slot for a live move of id to this node and returns this node's id.
func (m *Manager) AdmitMove(ctx context.Context, id string, a MoveAdmission) (string, error) {
	if m.moves == nil {
		return "", errMovesOff
	}
	if err := m.validate(a.Key); err != nil {
		return "", fmt.Errorf("%w: %w", ErrNotMovable, err)
	}
	if err := m.tenantRemoved(ctx, a.Tenant); err != nil {
		return "", err
	}
	m.mu.Lock()
	err := m.admitMoveLocked(id, a)
	m.mu.Unlock()
	if err != nil {
		return "", err
	}
	dir := m.moveDir(id)
	if err = os.RemoveAll(dir); err == nil {
		err = os.MkdirAll(dir, 0o700)
	}
	if err != nil {
		m.DropMove(id)
		return "", fmt.Errorf("move %s: staging dir: %w", id, err)
	}
	return m.nodeID, nil
}

// ReceiveMoveRound writes one round of a reserved move into its staging dir.
func (m *Manager) ReceiveMoveRound(id string, body io.Reader) error {
	in := m.beginReceive(id)
	if in == nil {
		return ErrUnknownSandbox
	}
	err := peer.ReceiveRound(body, m.moveDir(id))
	if current, reserved := m.endReceive(id, in); !current {
		if !reserved {
			_ = os.RemoveAll(m.moveDir(id))
		}
		return ErrUnknownSandbox
	}
	if err != nil {
		return fmt.Errorf("move %s: %w", id, err)
	}
	return nil
}

// CommitMove receives the staged series as the claim, commits the move record and starts the guest.
func (m *Manager) CommitMove(ctx context.Context, id string, claim []byte, epoch int64) error {
	ctx = context.WithoutCancel(ctx)
	admitted := m.beginReceive(id)
	if admitted == nil {
		return fmt.Errorf("%w: no reservation for the move of %s", ErrNotMovable, id)
	}
	defer m.endReceive(id, admitted)
	if m.memoryLow.Load() {
		return fmt.Errorf("%w: %s", ErrQuota, memoryLowReason)
	}
	sb := &types.Sandbox{}
	if err := json.Unmarshal(claim, sb); err != nil || sb.ID != id {
		return fmt.Errorf("%w: the claim record does not decode as %s", ErrNotMovable, id)
	}
	name := vmName(sb.Key)
	rec, err := m.eng.MigrateReceive(ctx, m.moveDir(id), name, sb.Key)
	if err != nil {
		m.removeStaleVM(ctx, name, types.VMRecord{State: vmStateCreating})
		return fmt.Errorf("move %s: receive: %w", id, err)
	}
	sb.VMName, sb.VsockSocket, sb.TAP, sb.HibernateSnap, sb.PendingSnap = name, rec.VsockSocket, "", "", ""
	sb.ArchiveCk, sb.ArchiveEpoch, sb.ArchivePending, sb.ReplicaCk, sb.ReplicaPending = "", 0, "", "", ""
	sb.MoveOut = nil
	sb.SetEnv(sb.Env)
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	m.mu.Lock()
	if m.incoming[id] != admitted || m.claimed[id] != nil {
		m.mu.Unlock()
		m.destroy(ctx, name)
		return fmt.Errorf("%w: the reservation for %s ended", ErrNotMovable, id)
	}
	sb.MoveIn = true
	delete(m.incoming, id)
	m.addClaimLocked(sb)
	js := m.store.set(sb)
	m.mu.Unlock()
	if err = m.store.commit(js); err != nil {
		m.dropMoveIn(ctx, sb)
		return fmt.Errorf("move %s: persist claims: %w", id, err)
	}
	committed, err := m.moves.Advance(ctx, id, epoch, moves.Final, moves.Committed)
	switch {
	case errors.Is(err, moves.ErrLost):
		m.dropMoveIn(ctx, sb)
		return fmt.Errorf("%w: the source ended the move of %s", ErrNotMovable, id)
	case err != nil:
		go m.settleMoveIn(ctx, sb)
		return fmt.Errorf("move %s: %w", id, err)
	}
	m.startMoveIn(ctx, sb, admitted.from, committed)
	return nil
}

// DropMove ends a reservation and its staging dir; a claim a commit already journaled is left to its record.
func (m *Manager) DropMove(id string) {
	m.mu.Lock()
	_, held := m.incoming[id]
	delete(m.incoming, id)
	m.mu.Unlock()
	if held {
		_ = os.RemoveAll(m.moveDir(id))
	}
}

// A guest that cannot start fails the claim here, never back to the source; the caller holds sb.Transition.
func (m *Manager) startMoveIn(ctx context.Context, sb *types.Sandbox, from string, committed int64) {
	logger := log.WithFunc("pool.startMoveIn")
	var err error
	for range 2 {
		if err = m.eng.Start(ctx, sb.VMName); err == nil {
			break
		}
		if errors.Is(err, engine.ErrStopping) {
			return
		}
		logger.Warnf(ctx, "start moved claim %s: %v", sb.ID, err)
	}
	if err != nil {
		_, persistErr := m.commitIfLive(ctx, sb, func() { sb.MoveIn = false })
		sb.ExitRecorded = true
		m.recordUsage(ctx, usage.Event{Event: "vmm_exit", ID: sb.ID, VMName: sb.VMName, Tenant: sb.Tenant})
		m.failLocked(ctx, sb, reasonMoveStartFailed)
		_ = os.RemoveAll(m.moveDir(sb.ID))
		if committed != 0 && persistErr == nil {
			m.markMoveStarted(ctx, sb.ID, committed)
		}
		return
	}
	if sock, probeErr := m.probeReady(ctx, sb.VMName, sb.VsockSocket, claimProbeTimeout); probeErr != nil {
		logger.Warnf(ctx, "probe moved claim %s: %v", sb.ID, probeErr)
	} else {
		m.mu.Lock()
		sb.VsockSocket = sock
		m.mu.Unlock()
	}
	if proxyErr := m.out.ArmProxy(ctx, sb); proxyErr != nil {
		logger.Errorf(ctx, proxyErr, "arm egress proxy %s", sb.ID)
	}
	live, err := m.commitIfLive(ctx, sb, func() { sb.MoveIn = false })
	if !live {
		m.out.Disarm(sb.ID, true)
		return
	}
	if err != nil {
		logger.Warnf(ctx, "persist the start of moved claim %s: %v", sb.ID, err)
	}
	sb.Touch()
	m.recordUsage(ctx, usage.Event{Event: "move_in", ID: sb.ID, VMName: sb.VMName, Tenant: sb.Tenant, Deadline: m.deadlineOf(sb)})
	_ = os.RemoveAll(m.moveDir(sb.ID))
	if committed != 0 && err == nil {
		m.markMoveStarted(ctx, sb.ID, committed)
	}
	logger.Infof(ctx, "sandbox %s moved here from %s", sb.ID, from)
}

func (m *Manager) markMoveStarted(ctx context.Context, id string, epoch int64) {
	if _, err := m.moves.Advance(ctx, id, epoch, moves.Committed, moves.Started); err != nil {
		log.WithFunc("pool.markMoveStarted").Warnf(ctx, "mark the move of %s started, leaving it to the sweep: %v", id, err)
	}
}

func (m *Manager) dropMoveIn(ctx context.Context, sb *types.Sandbox) {
	if holds, ok := m.forgetClaim(ctx, sb); ok {
		m.out.Disarm(sb.ID, true)
		m.removeOrRetry(ctx, sb.VMName, "", volumeTeardown{holds: holds})
	}
	_ = os.RemoveAll(m.moveDir(sb.ID))
}

func (m *Manager) settleMoveIn(ctx context.Context, sb *types.Sandbox) {
	for !m.resolveMoveIn(ctx, sb) {
		time.Sleep(moveRetryInterval)
	}
}

// False while the registry is down.
func (m *Manager) resolveMoveIn(ctx context.Context, sb *types.Sandbox) bool {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	m.mu.Lock()
	pending := m.claimed[sb.ID] == sb && sb.MoveIn
	m.mu.Unlock()
	if !pending {
		return true
	}
	rec, ok, err := m.moves.Get(ctx, sb.ID)
	if err != nil {
		log.WithFunc("pool.resolveMoveIn").Warnf(ctx, "resolve moved claim %s, retrying: %v", sb.ID, err)
		return false
	}
	toHere := ok && rec.To == m.nodeID
	switch {
	case toHere && rec.State == moves.Committed:
		m.startMoveIn(ctx, sb, rec.From, rec.Epoch)
		return true
	case toHere && rec.State == moves.Started:
		m.startMoveIn(ctx, sb, rec.From, 0)
		return true
	case toHere && (rec.State == moves.Pending || rec.State == moves.Final):
		if _, err = m.moves.Advance(ctx, sb.ID, rec.Epoch, rec.State, moves.Aborted); err != nil {
			log.WithFunc("pool.resolveMoveIn").Warnf(ctx, "abort the move of %s, retrying: %v", sb.ID, err)
			return false
		}
	}
	m.dropMoveIn(ctx, sb)
	return true
}

func (m *Manager) beginReceive(id string) *inMove {
	m.mu.Lock()
	defer m.mu.Unlock()
	in := m.incoming[id]
	if in != nil {
		in.receiving++
	}
	return in
}

func (m *Manager) endReceive(id string, in *inMove) (current, reserved bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	in.receiving--
	in.touched = time.Now()
	held := m.incoming[id]
	return held == in, held != nil
}

func (m *Manager) dropIdleReservations() {
	now := time.Now()
	var idle []string
	m.mu.Lock()
	for id, in := range m.incoming {
		if in.receiving == 0 && now.Sub(in.touched) > moveReservationIdle {
			idle = append(idle, id)
		}
	}
	m.mu.Unlock()
	for _, id := range idle {
		m.DropMove(id)
	}
}

// Callers hold m.mu.
func (m *Manager) incomingOf(tenant string) int {
	n := 0
	for _, in := range m.incoming {
		if in.tenant == tenant {
			n++
		}
	}
	return n
}

func (m *Manager) admitMoveLocked(id string, a MoveAdmission) error {
	if _, held := m.claimed[id]; held {
		return fmt.Errorf("%w: this node already holds %s", ErrNotMovable, id)
	}
	extra := 1
	if in, held := m.incoming[id]; held {
		if in.from != a.From {
			return fmt.Errorf("%w: a move of %s to this node is already running", ErrNotMovable, id)
		}
		extra = 0
	} else if err := m.moveBudgetErr(); err != nil {
		return err
	}
	if err := m.quotaErr(extra, a.Tenant); err != nil {
		return err
	}
	m.incoming[id] = &inMove{tenant: a.Tenant, from: a.From, touched: time.Now()}
	return nil
}
