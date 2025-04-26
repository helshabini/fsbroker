package fsbroker

import "sync"

type Map struct {
	mu    sync.RWMutex
	items map[string]*Info
}

func NewMap() *Map {
	return &Map{
		items: make(map[string]*Info),
	}
}

func (m *Map) Set(key string, value *Info) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[key] = value
}

func (m *Map) Get(key string) *Info {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.items[key]
}

func (m *Map) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, key)
}

func (m *Map) Iterate(callback func(key string, value *Info)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for key, value := range m.items {
		callback(key, value)
	}
}

func (m *Map) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.items)
}

func (m *Map) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = make(map[string]*Info)
}
