package storage

import "testing"

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
