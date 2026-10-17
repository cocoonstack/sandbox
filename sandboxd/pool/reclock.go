package pool

import "sync"

// recLock takes the per-record lock and a live reference; pair with recDone.
func (m *Manager) recLock(id string) *sync.RWMutex {
	m.recLocksMu.Lock()
	defer m.recLocksMu.Unlock()
	m.recRefs[id]++
	l := m.recLocks[id]
	if l == nil {
		l = &sync.RWMutex{}
		m.recLocks[id] = l
	}
	return l
}

func (m *Manager) lockRec(id string) func() {
	l := m.recLock(id)
	l.Lock()
	return func() { l.Unlock(); m.recDone(id) }
}

func (m *Manager) rlockRec(id string) func() {
	l := m.recLock(id)
	l.RLock()
	return func() { l.RUnlock(); m.recDone(id) }
}

// recDone releases a reference taken by recLock; call after Unlock/RUnlock, never before.
func (m *Manager) recDone(id string) {
	m.recLocksMu.Lock()
	defer m.recLocksMu.Unlock()
	m.recRefs[id]--
	if m.recRefs[id] <= 0 {
		delete(m.recRefs, id)
		delete(m.recLocks, id)
	}
}
