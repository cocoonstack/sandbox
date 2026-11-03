package pool

import "sync"

// recordLocks serializes same-id record mutations; an entry lives exactly as long as a recLock reference to it.
type recordLocks struct {
	mu   sync.Mutex
	held map[string]*sync.RWMutex
	refs map[string]int
}

// recLock returns the per-record lock and takes a live reference; pair with recDone.
func (r *recordLocks) recLock(id string) *sync.RWMutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refs[id]++
	l := r.held[id]
	if l == nil {
		l = &sync.RWMutex{}
		r.held[id] = l
	}
	return l
}

func (r *recordLocks) lockRec(id string) func() {
	l := r.recLock(id)
	l.Lock()
	return func() { l.Unlock(); r.recDone(id) }
}

func (r *recordLocks) rlockRec(id string) func() {
	l := r.recLock(id)
	l.RLock()
	return func() { l.RUnlock(); r.recDone(id) }
}

// recDone releases a reference taken by recLock; call after Unlock/RUnlock, never before.
func (r *recordLocks) recDone(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refs[id]--
	if r.refs[id] <= 0 {
		delete(r.refs, id)
		delete(r.held, id)
	}
}
