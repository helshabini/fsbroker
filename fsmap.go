package fsbroker

import (
	"errors"
	"sync"
	// "github.com/google/uuid" // No longer needed
)

type FSMap struct {
	mu    sync.RWMutex
	ids   map[uint64]*FSInfo
	paths map[string]*FSInfo
	// uuids map[string]*FSInfo // Removed redundant map
}

func NewFSMap() *FSMap {
	return &FSMap{
		ids:   make(map[uint64]*FSInfo),
		paths: make(map[string]*FSInfo),
		// uuids: make(map[string]*FSInfo), // Removed
	}
}

func (m *FSMap) Set(value *FSInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	/* // Removed UUID generation logic
	if len(value.UId) == 0 {
		uid, err := uuid.NewV6()
		if err != nil {
			return err
		}
		value.UId = uid.String()
	}
	*/

	// m.uuids[value.UId] = value // Removed
	m.ids[value.Id] = value
	m.paths[value.Path] = value

	return nil
}

func (m *FSMap) GetById(id uint64) *FSInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ids[id]
}

func (m *FSMap) GetByPath(path string) *FSInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.paths[path]
}

func (m *FSMap) DeleteById(id uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	value, found := m.ids[id]
	if !found {
		return errors.New("id not found")
	}

	delete(m.ids, id)
	delete(m.paths, value.Path)
	// delete(m.uuids, value.UId) // Removed

	return nil
}

func (m *FSMap) DeleteByPath(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	value, found := m.paths[path]
	if !found {
		return errors.New("path not found")
	}

	delete(m.ids, value.Id)
	delete(m.paths, path)
	// delete(m.uuids, value.UId) // Removed

	return nil
}

func (m *FSMap) IterateIds(callback func(key uint64, value *FSInfo)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for key, value := range m.ids {
		callback(key, value)
	}
}

func (m *FSMap) IteratePaths(callback func(key string, value *FSInfo)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for key, value := range m.paths {
		callback(key, value)
	}
}

func (m *FSMap) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Use length of ids or paths map instead
	return len(m.ids)
}
