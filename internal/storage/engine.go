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

	e := &Engine{
		memTable: newMemTable(),
		wal:      newWAL(walDirPath()),
		ssTable:  newSSTable(sstableDirPath()),
	}

	//load the wal into memory
	entries, err := e.wal.getAll()

	if err != nil {
		slog.Info("Error while loading the WAL", "err", err)
	}

	slog.Info("Processing wal entries", "Count", len(entries))
	for _, v := range entries {
		switch v.Type {
		case entryTypeDelete:
			e.memTable.delete(v.Key)
		case entryTypePut:
			e.memTable.put(v.Key, v.Value)
		default:
			slog.Info("Unknow entry in wal", "type", v.Type)
		}
	}

	return e
}

func (e *Engine) Get(key string) (string, bool) {
	slog.Info("Get called", "key", key)

	entry, ok := e.memTable.get(key)

	if ok {
		if entry.Type == entryTypeDelete {
			slog.Info("Key deleted in Mem Table", "key", key)
			return "", false
		}

		slog.Info("Key found in Mem Table", "key", key, "value", entry.Value)
		return entry.Value, true
	}

	// Get data from sstable
	entry, ok = e.ssTable.getKey(key)
	if ok {
		if entry.Type == entryTypeDelete {
			slog.Info("Key deleted in ss table", "key", key)
			return "", false
		}

		slog.Info("Key found in ss table", "key", key, "value", entry.Value)
		return entry.Value, true
	}

	return "", false
}

func (e *Engine) Put(key, value string) {
	slog.Info("Put called", "key", key, "value", value)

	entry, err := newWALEntry(key, value, entryTypePut)
	if err != nil {
		slog.Error("Failed to create WAL entry", "key", key, "error", err)
		return
	}
	if err := e.wal.append(entry); err != nil {
		slog.Error("Failed to append WAL entry", "key", key, "error", err)
		return
	}

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
		if err := e.wal.clear(); err != nil {
			//TODO stop server (recovery)?
			slog.Error("Error while deleting the wal file", "error", err)
			return
		}
	}

	slog.Info("Key inserted/updated", "key", key)
}

func (e *Engine) Delete(key string) {
	slog.Info("Delete called", "key", key)

	entry, err := newWALEntry(key, "", entryTypeDelete)
	if err != nil {
		slog.Error("Failed to create WAL entry", "key", key, "error", err)
		return
	}
	if err := e.wal.append(entry); err != nil {
		slog.Error("Failed to append WAL entry", "key", key, "error", err)
		return
	}

	e.memTable.delete(key)
}
