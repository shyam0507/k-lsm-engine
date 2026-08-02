package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
