package fsbroker

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Helper to create a test FSInfo
func newTestInfo(id uint64, path string) *FSInfo {
	return &FSInfo{
		Id:      id,
		Path:    path,
		Size:    100 + id, // Arbitrary size
		ModTime: time.Now(),
		Mode:    0644, // Arbitrary mode
	}
}

func TestNewFSMap(t *testing.T) {
	m := NewFSMap()
	if m == nil {
		t.Fatal("NewFSMap returned nil")
	}
	if len(m.ids) != 0 || len(m.paths) != 0 { // Removed check for m.uuids
		t.Errorf("Expected new map to be empty, got ids=%d, paths=%d", len(m.ids), len(m.paths))
	}
}

func TestFSMap_SetAndGet(t *testing.T) {
	m := NewFSMap()
	info1 := newTestInfo(1, "/path/to/file1")
	info2 := newTestInfo(2, "/path/to/file2")

	// Set first item
	err := m.Set(info1)
	if err != nil {
		t.Fatalf("Set failed for info1: %v", err)
	}

	// Verify retrieval
	retrievedById := m.GetById(1)
	if retrievedById != info1 {
		t.Errorf("GetById(1): Expected info1, got %v", retrievedById)
	}
	retrievedByPath := m.GetByPath("/path/to/file1")
	if retrievedByPath != info1 {
		t.Errorf("GetByPath(\"/path/to/file1\"): Expected info1, got %v", retrievedByPath)
	}
	if m.Size() != 1 {
		t.Errorf("Expected size 1, got %d", m.Size())
	}

	// Set second item
	err = m.Set(info2)
	if err != nil {
		t.Fatalf("Set failed for info2: %v", err)
	}

	// Verify retrieval
	retrievedById2 := m.GetById(2)
	if retrievedById2 != info2 {
		t.Errorf("GetById(2): Expected info2, got %v", retrievedById2)
	}
	retrievedByPath2 := m.GetByPath("/path/to/file2")
	if retrievedByPath2 != info2 {
		t.Errorf("GetByPath(\"/path/to/file2\"): Expected info2, got %v", retrievedByPath2)
	}
	if m.Size() != 2 {
		t.Errorf("Expected size 2, got %d", m.Size())
	}

	// Test getting non-existent items
	if m.GetById(99) != nil {
		t.Error("GetById(99): Expected nil for non-existent ID")
	}
	if m.GetByPath("/non/existent") != nil {
		t.Error("GetByPath(\"/non/existent\"): Expected nil for non-existent path")
	}

	// Test updating an item (by setting again with same ID/Path but different data)
	updatedInfo1 := newTestInfo(1, "/path/to/file1") // Same ID and Path
	updatedInfo1.Size = 999

	err = m.Set(updatedInfo1) // Should overwrite
	if err != nil {
		t.Fatalf("Set failed for updatedInfo1: %v", err)
	}

	// Verify update
	retrievedAfterUpdate := m.GetById(1)
	if retrievedAfterUpdate != updatedInfo1 {
		t.Errorf("GetById(1) after update: Expected updatedInfo1, got %v", retrievedAfterUpdate)
	}
	if retrievedAfterUpdate == info1 {
		t.Error("GetById(1) after update: Retrieved original info1, not updated")
	}
	if retrievedAfterUpdate.Size != 999 {
		t.Errorf("GetById(1) after update: Expected size 999, got %d", retrievedAfterUpdate.Size)
	}
	if m.Size() != 2 { // Size should not increase
		t.Errorf("Expected size 2 after update, got %d", m.Size())
	}

	// Verify internal maps are consistent
	if m.ids[1] != updatedInfo1 {
		t.Error("Internal ids map not updated correctly")
	}
	if m.paths["/path/to/file1"] != updatedInfo1 {
		t.Error("Internal paths map not updated correctly")
	}
}

func TestFSMap_Delete(t *testing.T) {
	m := NewFSMap()
	info1 := newTestInfo(1, "/path/to/file1")
	info2 := newTestInfo(2, "/path/to/file2")
	info3 := newTestInfo(3, "/path/to/file3")

	_ = m.Set(info1)
	_ = m.Set(info2)
	_ = m.Set(info3)

	if m.Size() != 3 {
		t.Fatalf("Expected initial size 3, got %d", m.Size())
	}

	// Delete by ID (info2)
	err := m.DeleteById(2)
	if err != nil {
		t.Errorf("DeleteById(2) returned unexpected error: %v", err)
	}
	if m.Size() != 2 {
		t.Errorf("Expected size 2 after DeleteById(2), got %d", m.Size())
	}
	if m.GetById(2) != nil {
		t.Error("GetById(2) should return nil after deletion")
	}
	if m.GetByPath("/path/to/file2") != nil {
		t.Error("GetByPath(\"/path/to/file2\"): should return nil after deletion")
	}

	// Delete by Path (info1)
	err = m.DeleteByPath("/path/to/file1")
	if err != nil {
		t.Errorf("DeleteByPath(\"/path/to/file1\"): returned unexpected error: %v", err)
	}
	if m.Size() != 1 {
		t.Errorf("Expected size 1 after DeleteByPath(\"/path/to/file1\"), got %d", m.Size())
	}
	if m.GetById(1) != nil {
		t.Error("GetById(1) should return nil after deletion")
	}
	if m.GetByPath("/path/to/file1") != nil {
		t.Error("GetByPath(\"/path/to/file1\"): should return nil after deletion")
	}

	// Verify remaining item (info3)
	if m.GetById(3) != info3 {
		t.Error("GetById(3) did not return info3")
	}
	if m.GetByPath("/path/to/file3") != info3 {
		t.Error("GetByPath(\"/path/to/file3\"): did not return info3")
	}

	// Delete non-existent ID
	err = m.DeleteById(99)
	if err == nil {
		t.Error("DeleteById(99) expected error for non-existent ID, got nil")
	}
	if m.Size() != 1 {
		t.Errorf("Size should remain 1 after deleting non-existent ID, got %d", m.Size())
	}

	// Delete non-existent Path
	err = m.DeleteByPath("/non/existent")
	if err == nil {
		t.Error("DeleteByPath(\"/non/existent\"): expected error for non-existent path, got nil")
	}
	if m.Size() != 1 {
		t.Errorf("Size should remain 1 after deleting non-existent path, got %d", m.Size())
	}

	// Delete last item
	err = m.DeleteById(3)
	if err != nil {
		t.Errorf("DeleteById(3) returned unexpected error: %v", err)
	}
	if m.Size() != 0 {
		t.Errorf("Expected size 0 after deleting last item, got %d", m.Size())
	}
	if len(m.ids) != 0 || len(m.paths) != 0 { // Removed check for m.uuids
		t.Error("Expected internal maps to be empty after deleting last item")
	}
}

func TestFSMap_Iterate(t *testing.T) {
	m := NewFSMap()
	info1 := newTestInfo(1, "/path/to/file1")
	info2 := newTestInfo(2, "/path/to/file2")
	expectedInfos := map[uint64]*FSInfo{1: info1, 2: info2}
	expectedPaths := map[string]*FSInfo{"/path/to/file1": info1, "/path/to/file2": info2}

	// Test empty iteration
	var countIdsEmpty int
	m.IterateIds(func(key uint64, value *FSInfo) {
		countIdsEmpty++
	})
	if countIdsEmpty != 0 {
		t.Errorf("IterateIds on empty map: Expected count 0, got %d", countIdsEmpty)
	}
	var countPathsEmpty int
	m.IteratePaths(func(key string, value *FSInfo) {
		countPathsEmpty++
	})
	if countPathsEmpty != 0 {
		t.Errorf("IteratePaths on empty map: Expected count 0, got %d", countPathsEmpty)
	}

	// Add items
	_ = m.Set(info1)
	_ = m.Set(info2)

	// Test IterateIds
	foundIds := make(map[uint64]*FSInfo)
	m.IterateIds(func(key uint64, value *FSInfo) {
		foundIds[key] = value
	})
	if !reflect.DeepEqual(foundIds, expectedInfos) {
		t.Errorf("IterateIds: Expected %v, got %v", expectedInfos, foundIds)
	}

	// Test IteratePaths
	foundPaths := make(map[string]*FSInfo)
	m.IteratePaths(func(key string, value *FSInfo) {
		foundPaths[key] = value
	})
	if !reflect.DeepEqual(foundPaths, expectedPaths) {
		t.Errorf("IteratePaths: Expected %v, got %v", expectedPaths, foundPaths)
	}
}

func TestFSMap_Concurrency(t *testing.T) {
	m := NewFSMap()
	numGoroutines := 50
	itemsPerGoroutine := 100
	var wg sync.WaitGroup

	// Concurrent Set
	wg.Add(numGoroutines)
	for i := range numGoroutines {
		go func(gIndex int) {
			defer wg.Done()
			for j := range itemsPerGoroutine {
				id := uint64(gIndex*itemsPerGoroutine + j)
				path := fmt.Sprintf("/path/gr%d/file%d", gIndex, j)
				info := newTestInfo(id, path)
				err := m.Set(info)
				if err != nil {
					t.Errorf("Concurrent Set failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()

	expectedSize := numGoroutines * itemsPerGoroutine
	if m.Size() != expectedSize {
		t.Errorf("Expected size %d after concurrent sets, got %d", expectedSize, m.Size())
	}

	// Concurrent Get, Delete, Iterate (mix)
	var deleteAttempts int64
	var deleteSuccesses int64
	var deleteFailures int64
	wg.Add(numGoroutines)
	for i := range numGoroutines {
		go func(gIndex int) {
			defer wg.Done()
			for j := range itemsPerGoroutine {
				id := uint64(gIndex*itemsPerGoroutine + j)
				path := fmt.Sprintf("/path/gr%d/file%d", gIndex, j)

				// Mix operations
				opType := (gIndex + j) % 4
				switch opType {
				case 0: // GetById
					info := m.GetById(id)
					// Basic check: if it exists, path should match roughly
					if info != nil && info.Path != path {
						t.Errorf("Concurrent GetById(%d) mismatch: Expected path %s, got %s", id, path, info.Path)
					}
				case 1: // GetByPath
					info := m.GetByPath(path)
					// Basic check: if it exists, ID should match roughly
					if info != nil && info.Id != id {
						t.Errorf("Concurrent GetByPath(%s) mismatch: Expected id %d, got %d", path, id, info.Id)
					}
				case 2: // DeleteById (delete ~half the items)
					if j%2 == 0 {
						atomic.AddInt64(&deleteAttempts, 1)
						err := m.DeleteById(id) // Ignore error as another goroutine might delete first
						if err == nil {
							atomic.AddInt64(&deleteSuccesses, 1)
						} else {
							// We expect errors if another goroutine deleted it first
							atomic.AddInt64(&deleteFailures, 1)
						}
					}
				case 3: // Iterate (just run it to check for race conditions)
					m.IteratePaths(func(k string, v *FSInfo) {})
					m.IterateIds(func(k uint64, v *FSInfo) {})
				}
			}
		}(i)
	}
	wg.Wait()

	t.Logf("Delete attempts: %d, Successes: %d, Failures (e.g. not found): %d", deleteAttempts, deleteSuccesses, deleteFailures)

	// Check final size - should be roughly half the initial size due to deletions
	finalSize := m.Size()
	// Allow some leeway due to race conditions in deletion attempts
	expectedDeletions := deleteSuccesses // The number of successful deletions should reflect the size change
	expectedFinalSize := int64(expectedSize) - expectedDeletions

	if int64(finalSize) != expectedFinalSize {
		t.Errorf("Expected final size %d (initial %d - success %d), got %d",
			expectedFinalSize, expectedSize, deleteSuccesses, finalSize)
	}

	t.Logf("Final size after concurrent ops: %d", finalSize)

	// Final check: try deleting remaining items by path to ensure consistency
	remainingPaths := make([]string, 0)
	m.IteratePaths(func(k string, v *FSInfo) {
		remainingPaths = append(remainingPaths, k)
	})

	deleteFailuresAfter := 0
	for _, p := range remainingPaths {
		err := m.DeleteByPath(p)
		if err != nil {
			// This *could* happen if a DeleteById raced and deleted it first, log but don't fail hard
			t.Logf("Cleanup DeleteByPath(%s) failed (might be race condition): %v", p, err)
			deleteFailuresAfter++
		}
	}

	if m.Size() != 0 {
		t.Errorf("Expected size 0 after final cleanup, got %d (Cleanup Delete Failures: %d)", m.Size(), deleteFailuresAfter)
	}
}
