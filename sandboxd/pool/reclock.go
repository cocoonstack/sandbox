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

// recDone releases a reference taken by recLock; call after Unlock/RUnlock, never before.
func (m *Manager) recDone(id string) {
	m.recLocksMu.Lock()
	defer m.recLocksMu.Unlock()
	m.recRefs[id]--
	if m.recRefs[id] <= 0 {
		delete(m.recRefs, id)
		m.evictIfPending(id)
	}
}

// recDoneEvict is recDone for a just-deleted record: the lock slot goes at zero references.
func (m *Manager) recDoneEvict(id string) {
	m.recLocksMu.Lock()
	defer m.recLocksMu.Unlock()
	m.recRefs[id]--
	if m.recRefs[id] <= 0 {
		delete(m.recRefs, id)
		delete(m.recEvict, id)
		delete(m.recLocks, id)
		return
	}
	// a holder or waiter remains; the call that drops the last reference evicts instead
	m.recEvict[id] = struct{}{}
}

// evictIfPending drops id's lock entry if a delete asked for eviction; callers hold recLocksMu.
func (m *Manager) evictIfPending(id string) {
	if _, ok := m.recEvict[id]; ok {
		delete(m.recEvict, id)
		delete(m.recLocks, id)
	}
}
