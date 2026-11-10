package pool

import (
	"context"
	"errors"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// SetBalloon sets a running claim's balloon target through cocoon and returns cocoon's report; the caller restores the reply's default.
func (m *Manager) SetBalloon(ctx context.Context, id, target string) (types.VMBalloon, error) {
	if !m.balloonReady() {
		return types.VMBalloon{}, ErrBalloonOff
	}
	m.mu.Lock()
	sb := m.claimed[id]
	var err error
	switch {
	case sb == nil:
		err = ErrUnknownSandbox
	case m.midMoveLocked(sb):
		err = ErrEvacuating
	case sb.Failed != "":
		err = ErrFailed
	case sb.ArchiveCk != "":
		err = ErrArchived
	case !runningShaped(sb) || sb.Waking:
		err = ErrPaused
	}
	var vm string
	if err == nil {
		vm = sb.VMName
	}
	m.mu.Unlock()
	if err != nil {
		return types.VMBalloon{}, err
	}
	b, err := m.eng.Balloon(ctx, vm, target)
	if errors.Is(err, engine.ErrBalloonUnsupported) {
		return types.VMBalloon{}, ErrNoBalloon
	}
	return b, err
}

func (m *Manager) balloonReady() bool {
	if m.balloonOK.Load() {
		return true
	}
	ctx := context.Background()
	if err := m.eng.CheckBalloonSupport(ctx); err != nil {
		log.WithFunc("pool.balloonReady").Error(ctx, err, "the balloon verb is off")
		return false
	}
	m.balloonOK.Store(true)
	return true
}
