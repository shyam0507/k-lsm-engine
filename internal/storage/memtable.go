package storage

import (
	"log/slog"
	"sync"
)

type memTable struct {
	kv map[string]string
	mu sync.RWMutex
}

func newMemTable() *memTable {
	slog.Info("Creating new memTable instance")
	return &memTable{
		kv: make(map[string]string),
	}
}

func (mem *memTable) get(key string) (string, bool) {
	slog.Info("memTable get called", "key", key)
	mem.mu.RLock()
	defer mem.mu.RUnlock()

	val := mem.kv[key]
	return val, val != ""
}

func (mem *memTable) put(key, value string) {
	slog.Info("memTable put called", "key", key, "value", value)
	mem.mu.Lock()
	defer mem.mu.Unlock()

	mem.kv[key] = value
}

func (mem *memTable) getAll() map[string]string {
	slog.Info("memTable getAll called")
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	return mem.kv
}
