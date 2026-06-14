package storage

import (
	"log/slog"
)

// TODO expose the value via env
const (
	FLUSH_THRESHOLD = 2000
)

type Engine struct {
	memTable *memTable
	wal      *wal
	ssTable  *ssTable
}

func NewEngine() *Engine {
	slog.Info("Creating new Engine instance")

	return &Engine{
		memTable: newMemTable(),
		wal:      NewWAL(walDirPath()),
		ssTable:  NewSSTable(sstableDirPath()),
	}
}

func (e *Engine) Get(key string) (string, bool) {
	slog.Info("Get called", "key", key)

	val, ok := e.memTable.get(key)

	if ok {
		slog.Info("Key found in Mem Table", "key", key, "value", val)
		return val, ok
	}

	// Get data from sstable
	val, ok = e.ssTable.getKey(key)
	if ok {
		slog.Info("Key found in ss table", "key", key, "value", val)
		return val, ok
	}

	return val, ok
}

func (e *Engine) Put(key, value string) {
	slog.Info("Put called", "key", key, "value", value)

	e.memTable.put(key, value)

	if len(e.memTable.kv) == FLUSH_THRESHOLD {
		slog.Info("Flush threshold reached, calling SaveSSTable", "count", len(e.memTable.kv))
		err := e.ssTable.saveSSTable(e.memTable.getAll())
		if err != nil {
			slog.Error("SaveSSTable failed", "error", err)
		} else {
			slog.Info("SaveSSTable succeeded, clearing in-memory map")
		}
		clear(e.memTable.kv)
	}

	slog.Info("Key inserted/updated", "key", key)
}

func (e *Engine) Delete(key string) {
	slog.Info("Delete called", "key", key)

	// Add a tombstone
	e.memTable.put(key, "")
}
