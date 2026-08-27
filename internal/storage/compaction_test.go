package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompactionStoresAndUsesL1KeyRanges(t *testing.T) {
	dir := t.TempDir()
	sst := newSSTableStore(dir)

	for table := 0; table < 2; table++ {
		entries := make(map[string]storageEntry, FLUSH_THRESHOLD)
		for i := 0; i < FLUSH_THRESHOLD; i++ {
			key := fmt.Sprintf("key-%03d", table*FLUSH_THRESHOLD+i)
			entries[key] = storageEntry{Type: entryTypePut, Value: key}
		}
		if err := sst.saveLevel0SSTable(entries); err != nil {
			t.Fatalf("save L0 table %d: %v", table, err)
		}
	}

	if err := sst.compactSSTables(); err != nil {
		t.Fatalf("compactSSTables: %v", err)
	}

	l1Tables := sst.getLevelSSTables(1)
	if len(l1Tables) != 2 {
		t.Fatalf("expected two L1 tables, got %v", l1Tables)
	}
	manifest, err := os.ReadFile(filepath.Join(dir, MANIFEST_FILE_NAME))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	for _, table := range l1Tables {
		var line string
		for _, candidate := range strings.Split(string(manifest), "\n") {
			if strings.HasPrefix(candidate, table+"\t") {
				line = candidate
				break
			}
		}
		parsedTable, keyRange, hasRange, err := parseManifestTable(line, 1)
		if err != nil || parsedTable != table || !hasRange || keyRange.min > keyRange.max {
			t.Fatalf("manifest does not contain a valid range for %s:\n%s", table, manifest)
		}
	}

	// Reopening verifies that ranges are read from the manifest, not only held
	// in memory, and getKey remains correct when it skips out-of-range tables.
	reopened := newSSTableStore(dir)
	if got, ok := reopened.getKey("key-010"); !ok || got.Value != "key-010" {
		t.Fatalf("getKey after reopening = (%+v, %t), want key-010", got, ok)
	}
	if !l1TableMayContain("key-010", sstableKeyRange{min: "key-000", max: "key-199"}, true) {
		t.Fatal("expected in-range key to select its L1 table")
	}
	if l1TableMayContain("key-010", sstableKeyRange{min: "key-200", max: "key-399"}, true) {
		t.Fatal("expected out-of-range key to skip the L1 table")
	}
}

func TestCompactionPreservesTablesWhenAnEntryIsMalformed(t *testing.T) {
	dir := t.TempDir()
	sst := newSSTableStore(dir)

	valid := map[string]storageEntry{
		"key": {Type: entryTypePut, Value: "value"},
	}
	if err := sst.saveLevel0SSTable(valid); err != nil {
		t.Fatalf("save first sstable: %v", err)
	}
	if err := sst.saveLevel0SSTable(map[string]storageEntry{
		"key2": {Type: entryTypePut, Value: "value2"},
	}); err != nil {
		t.Fatalf("save second sstable: %v", err)
	}

	badPath := filepath.Join(dir, "l0", "sst-3.db")
	if err := os.WriteFile(badPath, []byte("{not-json}\n"), 0o644); err != nil {
		t.Fatalf("write malformed sstable: %v", err)
	}

	manifestPath := filepath.Join(dir, MANIFEST_FILE_NAME)
	if err := os.WriteFile(manifestPath, []byte("[L0]\nsst-1.db\nsst-2.db\nsst-3.db\n\n[L1]\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	sst.levels = []sstableLevel{{level: 0, tables: []string{"sst-1.db", "sst-2.db", "sst-3.db"}}, {level: 1, tables: nil}}

	if err := sst.compactSSTables(); err == nil {
		t.Fatal("expected compaction to reject malformed SSTable data")
	}
	if got := len(sst.getLevelSSTables(0)); got != 3 {
		t.Fatalf("expected source tables to remain after failed compaction, got %d", got)
	}
	if _, err := os.Stat(badPath); err != nil {
		t.Fatalf("expected malformed source table to remain: %v", err)
	}
}

func TestCompactionPersistsPruningWhenTooFewLevelZeroTablesRemain(t *testing.T) {
	dir := t.TempDir()
	sst := newSSTableStore(dir)

	if err := sst.saveLevel0SSTable(map[string]storageEntry{"k1": {Type: entryTypePut, Value: "v1"}}); err != nil {
		t.Fatalf("save sstable: %v", err)
	}
	sst.levels = []sstableLevel{{level: 0, tables: []string{"sst-1.db", "sst-99.db"}}}

	if err := sst.compactSSTables(); err != nil {
		t.Fatalf("compactSSTables returned error: %v", err)
	}
	if got := sst.getLevelSSTables(0); len(got) != 1 || got[0] != "sst-1.db" {
		t.Fatalf("expected stale table to be pruned, got %v", got)
	}
	manifest, err := os.ReadFile(filepath.Join(dir, MANIFEST_FILE_NAME))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if strings.Contains(string(manifest), "sst-99.db") {
		t.Fatalf("manifest retained stale table: %s", manifest)
	}
}

func TestCompactionPrunesMissingTablesFromManifestState(t *testing.T) {
	dir := t.TempDir()
	sst := newSSTableStore(dir)

	if err := sst.saveLevel0SSTable(map[string]storageEntry{"k1": {Type: entryTypePut, Value: "v1"}}); err != nil {
		t.Fatalf("save first sstable: %v", err)
	}
	if err := sst.saveLevel0SSTable(map[string]storageEntry{"k2": {Type: entryTypePut, Value: "v2"}}); err != nil {
		t.Fatalf("save second sstable: %v", err)
	}

	sst.levels = []sstableLevel{
		{level: 0, tables: []string{"sst-1.db", "sst-2.db"}},
		{level: 1, tables: []string{"sst-99.db"}},
	}

	if err := sst.compactSSTables(); err != nil {
		t.Fatalf("compactSSTables returned error: %v", err)
	}

	if got := len(sst.getLevelSSTables(0)); got != 0 {
		t.Fatalf("expected L0 to be cleared after compaction, got %d tables", got)
	}

	if got := len(sst.getLevelSSTables(1)); got == 0 {
		t.Fatal("expected compaction to produce at least one L1 table")
	}

	manifestBytes, err := os.ReadFile(filepath.Join(dir, MANIFEST_FILE_NAME))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if strings.Contains(string(manifestBytes), "sst-99.db") {
		t.Fatalf("manifest should not contain stale missing table, got:\n%s", string(manifestBytes))
	}
}
