package storage

import (
	"fmt"
	"testing"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()

	return &Engine{
		memTable: newMemTable(),
		wal:      newWAL(t.TempDir()),
		ssTable:  newSSTable(t.TempDir()),
	}
}

func TestDeleteHidesMemTableValue(t *testing.T) {
	engine := newTestEngine(t)

	engine.Put("key", "value")
	engine.Delete("key")

	value, ok := engine.Get("key")
	if ok {
		t.Fatalf("expected deleted key to be missing, got value %q", value)
	}
}

func TestSSTableTombstoneHidesOlderValue(t *testing.T) {
	dir := t.TempDir()
	sst := newSSTable(dir)

	if err := sst.saveSSTable(map[string]storageEntry{
		"key": {Type: entryTypePut, Value: "value"},
	}); err != nil {
		t.Fatalf("save put sstable: %v", err)
	}

	if err := sst.saveSSTable(map[string]storageEntry{
		"key": {Type: entryTypeDelete},
	}); err != nil {
		t.Fatalf("save delete sstable: %v", err)
	}

	engine := &Engine{
		memTable: newMemTable(),
		wal:      newWAL(t.TempDir()),
		ssTable:  sst,
	}

	value, ok := engine.Get("key")
	if ok {
		t.Fatalf("expected tombstoned key to be missing, got value %q", value)
	}
}

func TestEmptyStringValueIsNotTombstone(t *testing.T) {
	engine := newTestEngine(t)

	engine.Put("key", "")

	value, ok := engine.Get("key")
	if !ok {
		t.Fatal("expected empty string value to be found")
	}
	if value != "" {
		t.Fatalf("expected empty string value, got %q", value)
	}
}

func TestPutFlushesMoreThanOnce(t *testing.T) {
	engine := newTestEngine(t)

	totalWrites := FLUSH_THRESHOLD * 2
	for i := 0; i < totalWrites; i++ {
		engine.Put(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
	}

	if got := len(engine.ssTable.tables); got != 2 {
		t.Fatalf("expected 2 sstables after two flush cycles, got %d", got)
	}
	if got := engine.memTable.size(); got != 0 {
		t.Fatalf("expected memtable to be empty after flush, got %d entries", got)
	}

	for i := 0; i < totalWrites; i++ {
		key := fmt.Sprintf("key-%d", i)
		expected := fmt.Sprintf("value-%d", i)
		value, ok := engine.Get(key)
		if !ok {
			t.Fatalf("expected %s to be found", key)
		}
		if value != expected {
			t.Fatalf("expected %s value %q, got %q", key, expected, value)
		}
	}
}

func TestDeleteCanTriggerFlush(t *testing.T) {
	engine := newTestEngine(t)

	for i := 0; i < FLUSH_THRESHOLD-1; i++ {
		engine.Put(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
	}

	engine.Delete("deleted-key")

	if got := len(engine.ssTable.tables); got != 1 {
		t.Fatalf("expected delete to trigger flush and create 1 sstable, got %d", got)
	}
	if got := engine.memTable.size(); got != 0 {
		t.Fatalf("expected memtable to be empty after delete-triggered flush, got %d entries", got)
	}
}
