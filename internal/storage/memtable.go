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

func (mem *memTable) put(key, value string) {
	slog.Info("memTable put called", "key", key, "value", value)
	mem.mu.Lock()
	defer mem.mu.Unlock()

	mem.kv[key] = storageEntry{
		Type:  entryTypePut,
		Value: value,
	}
}

func (mem *memTable) delete(key string) {
	slog.Info("memTable delete called", "key", key)
	mem.mu.Lock()
	defer mem.mu.Unlock()

	mem.kv[key] = storageEntry{
		Type: entryTypeDelete,
	}
}

func (mem *memTable) getAll() map[string]storageEntry {
	slog.Info("memTable getAll called")
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	return mem.kv
}
