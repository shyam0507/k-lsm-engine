package storage

import (
	"log/slog"
	"sync"
)

type memTable struct {
	kv map[string]storageEntry
	mu sync.RWMutex
}

func newMemTable() *memTable {
	slog.Info("Creating new memTable instance")
	return &memTable{
		kv: make(map[string]storageEntry),
	}
}

func (mem *memTable) get(key string) (storageEntry, bool) {
	slog.Info("memTable get called", "key", key)
	mem.mu.RLock()
	defer mem.mu.RUnlock()

	entry, ok := mem.kv[key]
	return entry, ok
}

func (mem *memTable) put(key, value string) int {
	slog.Info("memTable put called", "key", key, "value", value)
	mem.mu.Lock()
	defer mem.mu.Unlock()

	mem.kv[key] = storageEntry{
		Type:  entryTypePut,
		Value: value,
	}

	size := len(mem.kv)

	return size
}

func (mem *memTable) delete(key string) int {
	slog.Info("memTable delete called", "key", key)
	mem.mu.Lock()
	defer mem.mu.Unlock()

	mem.kv[key] = storageEntry{
		Type: entryTypeDelete,
	}

	return len(mem.kv)
}

func (mem *memTable) getAll() map[string]storageEntry {
	slog.Info("memTable getAll called")
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	return mem.kv
}

func (mem *memTable) size() int {
	mem.mu.RLock()
	defer mem.mu.RUnlock()

	return len(mem.kv)
}

func (mem *memTable) clear() {
	slog.Info("memTable clear called")
	mem.mu.Lock()
	defer mem.mu.Unlock()

	clear(mem.kv)
}
