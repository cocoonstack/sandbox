package sandbox

import (
	"context"
	"sync"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

// Watcher is an open filesystem watch. Events yields events until the watch
// ends — by Close, ctx cancellation, or a watcher error on the sandbox side —
// after which Err reports the terminal error (nil after a clean Close).
type Watcher struct {
	events chan wire.Event
	stop   func()
	closed chan struct{}
	once   sync.Once

	mu  sync.Mutex
	err error
}

// Events yields filesystem events; the channel closes when the watch ends.
func (w *Watcher) Events() <-chan wire.Event { return w.events }

// Close ends the watch (the sandbox side stops on disconnect).
func (w *Watcher) Close() error {
	w.once.Do(func() {
		close(w.closed)
		w.stop()
	})
	return nil
}

// Err reports why the event stream ended; nil after a clean Close.
func (w *Watcher) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

// drain relays event frames into the channel until a terminal condition; the
// closed channel keeps it from blocking on a consumer that stopped reading.
func (w *Watcher) drain(ctx context.Context, conn *silkd.Conn) {
	defer close(w.events)
	for {
		resp, err := recv(ctx, conn)
		if err != nil {
			select {
			case <-w.closed: // clean Close: the conn error is our own doing
			default:
				w.setErr(err)
			}
			return
		}
		switch r := resp.(type) {
		case *wire.Event:
			select {
			case w.events <- *r:
			case <-w.closed:
				return
			}
		case *wire.ErrorResp:
			w.setErr(r)
			return
		default:
			w.setErr(unexpected(resp))
			return
		}
	}
}

func (w *Watcher) setErr(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.err = err
}

// Watch streams filesystem events under path (recursively when set). It
// returns once the sandbox acknowledges the watch is armed, so events caused
// after Watch returns are guaranteed captured.
func (s *Sandbox) Watch(ctx context.Context, path string, recursive bool) (*Watcher, error) {
	conn, done, err := s.call(ctx, &wire.FsWatch{Path: path, Recursive: recursive})
	if err != nil {
		return nil, err
	}
	if _, err = expect[wire.Ready](ctx, conn); err != nil {
		done()
		return nil, err
	}
	w := &Watcher{
		events: make(chan wire.Event, 16),
		stop:   done,
		closed: make(chan struct{}),
	}
	go w.drain(ctx, conn)
	return w, nil
}
