package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBloomFilterSidecarRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sst-1.db")
	entries := []ssTableEntry{
		{K: "alpha", V: "1", Type: entryTypePut},
		{K: "bravo", V: "2", Type: entryTypePut},
	}
	if err := writeBloomFilterSidecar(path, bloomFilterForEntries(entries)); err != nil {
		t.Fatalf("write bloom sidecar: %v", err)
	}

	filter, err := readBloomFilterSidecar(path)
	if err != nil {
		t.Fatalf("read bloom sidecar: %v", err)
	}
	for _, key := range []string{"alpha", "bravo"} {
		if !filter.found(key) {
			t.Fatalf("expected bloom filter to contain %q", key)
		}
	}
	if filter.found("not-present") {
		t.Fatal("unexpected false positive for a sparse bloom filter")
	}
}

func TestMissingOrCorruptBloomSidecarFallsBackToSSTableRead(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(t *testing.T, path string)
	}{
		{
			name: "missing",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Remove(path); err != nil {
					t.Fatalf("remove bloom sidecar: %v", err)
				}
			},
		},
		{
			name: "corrupt",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				if err := os.WriteFile(path, []byte("corrupt"), 0644); err != nil {
					t.Fatalf("corrupt bloom sidecar: %v", err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			sst := newSSTableStore(dir)
			if err := sst.saveLevel0SSTable(map[string]storageEntry{
				"key": {Type: entryTypePut, Value: "value"},
			}); err != nil {
				t.Fatalf("save sstable: %v", err)
			}

			table := sst.getLevelSSTables(0)[0]
			test.mutate(t, bloomSidecarPath(filepath.Join(dir, "l0", table)))

			reloaded := newSSTableStore(dir)
			entry, ok := reloaded.getKey("key")
			if !ok || entry.Type != entryTypePut || entry.Value != "value" {
				t.Fatalf("fallback read = (%#v, %t), want stored value", entry, ok)
			}
		})
	}
}

func TestCompactionRemovesOldBloomSidecars(t *testing.T) {
	dir := t.TempDir()
	sst := newSSTableStore(dir)
	for _, entries := range []map[string]storageEntry{
		{"alpha": {Type: entryTypePut, Value: "1"}},
		{"bravo": {Type: entryTypePut, Value: "2"}},
	} {
		if err := sst.saveLevel0SSTable(entries); err != nil {
			t.Fatalf("save level 0 table: %v", err)
		}
	}
	oldTables := sst.getLevelSSTables(0)
	if err := sst.compactSSTables(); err != nil {
		t.Fatalf("compact sstables: %v", err)
	}

	for _, table := range oldTables {
		if _, err := os.Stat(bloomSidecarPath(filepath.Join(dir, "l0", table))); !os.IsNotExist(err) {
			t.Fatalf("old bloom sidecar %q still exists or could not be checked: %v", table, err)
		}
	}
	newTables := sst.getLevelSSTables(1)
	if len(newTables) == 0 {
		t.Fatal("expected compaction to create a level 1 table")
	}
	if _, err := os.Stat(bloomSidecarPath(filepath.Join(dir, "l1", newTables[0]))); err != nil {
		t.Fatalf("new bloom sidecar missing: %v", err)
	}
}
