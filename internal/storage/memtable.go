package storage

import (
	"log/slog"
	"sync"
)

type memTable struct {
	kv skipList
	mu sync.RWMutex
}

func newMemTable() *memTable {
	slog.Info("Creating new memTable instance")
	return &memTable{
		kv: *newSkipList(),
	}
}

func (mem *memTable) get(key string) (storageEntry, bool) {
	slog.Info("memTable get called", "key", key)
	mem.mu.RLock()
	defer mem.mu.RUnlock()

	entry, ok := mem.kv.get(key)
	return entry, ok
}

func (mem *memTable) put(key, value string) int {
	slog.Info("memTable put called", "key", key, "value", value)
	mem.mu.Lock()
	defer mem.mu.Unlock()

	mem.kv.add(key, storageEntry{
		Type:  entryTypePut,
		Value: value,
	})
	return mem.kv.size
}

func (mem *memTable) delete(key string) int {
	slog.Info("memTable delete called", "key", key)
	mem.mu.Lock()
	defer mem.mu.Unlock()

	mem.kv.add(key, storageEntry{Type: entryTypeDelete})
	return mem.kv.size
}

func (mem *memTable) entries() []ssTableEntry {
	slog.Info("memTable entries called")
	mem.mu.RLock()
	defer mem.mu.RUnlock()

	return mem.kv.entries()
}

func (mem *memTable) size() int {
	mem.mu.RLock()
	defer mem.mu.RUnlock()

	return mem.kv.size
}

func (mem *memTable) clear() {
	slog.Info("memTable clear called")
	mem.mu.Lock()
	defer mem.mu.Unlock()

	mem.kv.clear()
}
