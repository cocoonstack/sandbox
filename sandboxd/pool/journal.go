package pool

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/projecteru2/core/log"
)

const (
	// journalMaxBytes rotates a journal once it crosses this size; one .1 backup
	// is kept so a slow collector never loses a window silently.
	journalMaxBytes = 64 * 1024 * 1024
	journalDepth    = 256
)

var errJournalClosed = errors.New("journal closed")

// usageEvent is one line of usage.jsonl: the product-level billing stream,
// folded by the platform collector (compute time = claim→release minus
// hibernate→wake; hibernated storage = hibernate→wake). VM names ride along
// so cocoon's machine-level metering ledger can audit the fold.
type usageEvent struct {
	Time      time.Time `json:"t"`
	Event     string    `json:"ev"` // claim|hibernate|wake|fork|promote|checkpoint|release|reap
	ID        string    `json:"id"`
	VMName    string    `json:"vm,omitempty"`
	KeyHash   string    `json:"key,omitempty"`      // claim only
	Tenant    string    `json:"tenant,omitempty"`   // claim only
	Children  []string  `json:"children,omitempty"` // fork only
	Reference string    `json:"ref,omitempty"`      // promote: template; checkpoint: ckpt id
}

// journal is an append-only JSONL writer with size rotation. append only
// enqueues: a single writer goroutine orders and persists events, keeping the
// marshal and write syscall off callers' paths (claim response, egress proxy).
type journal struct {
	mu      sync.RWMutex
	closed  bool
	ch      chan any
	drained chan struct{}

	path string
	f    *os.File
	size int64
}

func newJournal(path string) (*journal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) //nolint:gosec // under data_dir
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	j := &journal{
		ch:      make(chan any, journalDepth),
		drained: make(chan struct{}),
		path:    path,
		f:       f,
		size:    st.Size(),
	}
	go j.run()
	return j, nil
}

func (j *journal) append(v any) error {
	j.mu.RLock()
	defer j.mu.RUnlock()
	if j.closed {
		return errJournalClosed
	}
	j.ch <- v
	return nil
}

// flush blocks until every event enqueued before it is on disk.
func (j *journal) flush() {
	mark := make(chan struct{})
	if j.append(mark) != nil {
		return // closed: the writer already drained
	}
	<-mark
}

// close stops intake, drains buffered events to disk, and closes the file.
func (j *journal) close() error {
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		return nil
	}
	j.closed = true
	close(j.ch)
	j.mu.Unlock()
	<-j.drained
	return j.f.Close()
}

func (j *journal) run() {
	for v := range j.ch {
		if mark, ok := v.(chan struct{}); ok {
			close(mark)
			continue
		}
		if err := j.write(v); err != nil {
			log.WithFunc("pool.journal").Errorf(context.Background(), err, "append to %s", j.path)
		}
	}
	close(j.drained)
}

func (j *journal) write(v any) error {
	line, err := json.Marshal(v)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if j.size+int64(len(line)) > journalMaxBytes {
		if rotateErr := j.rotate(); rotateErr != nil {
			return rotateErr
		}
	}
	n, err := j.f.Write(line)
	j.size += int64(n)
	return err
}

// rotate moves the live file to .1 (replacing any prior backup) and reopens.
func (j *journal) rotate() error {
	if err := j.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(j.path, j.path+".1"); err != nil {
		return err
	}
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) //nolint:gosec // under data_dir
	if err != nil {
		return err
	}
	j.f, j.size = f, 0
	return nil
}
