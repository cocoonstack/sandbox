package pool

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"os"
	"sync"
	"time"
)

// journalMaxBytes rotates a journal once it crosses this size; one .1 backup is kept.
const journalMaxBytes = 64 * 1024 * 1024

// usageEvent is one line of usage.jsonl, the billing stream the platform collector folds.
type usageEvent struct {
	Time      time.Time `json:"t"`
	Event     string    `json:"ev"`
	ID        string    `json:"id"`
	VMName    string    `json:"vm,omitempty"`
	KeyHash   string    `json:"key,omitempty"`
	Tenant    string    `json:"tenant,omitempty"`
	Volumes   []string  `json:"volumes,omitempty"`
	VolumesRW []string  `json:"volumes_rw,omitempty"`
	Children  []string  `json:"children,omitempty"`
	Reference string    `json:"ref,omitempty"`
}

// journal is an append-only JSONL writer with size rotation; its lock only orders appends.
type journal struct {
	mu   sync.Mutex
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
	return &journal{path: path, f: f, size: st.Size()}, nil
}

func (j *journal) append(v any) error {
	line, err := json.Marshal(v, jsontext.AllowInvalidUTF8(true))
	if err != nil {
		return err
	}
	line = append(line, '\n')
	j.mu.Lock()
	defer j.mu.Unlock()
	var rotateErr error
	if j.size+int64(len(line)) > journalMaxBytes {
		rotateErr = j.rotate()
	}
	n, err := j.f.Write(line)
	j.size += int64(n)
	return errors.Join(rotateErr, err)
}

// rotate moves the live file to .1 and reopens; the old descriptor closes last.
func (j *journal) rotate() error {
	renameErr := os.Rename(j.path, j.path+".1")
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return errors.Join(renameErr, err)
	}
	old := j.f
	j.f, j.size = f, 0
	if renameErr != nil {
		if st, statErr := f.Stat(); statErr == nil {
			j.size = st.Size()
		}
	}
	return errors.Join(renameErr, old.Close())
}
