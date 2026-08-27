package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBlockBasedTableLookupAcrossBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.db")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]ssTableEntry, 0, 65)
	for i := 0; i < 65; i++ {
		entries = append(entries, ssTableEntry{K: fmt.Sprintf("key-%03d", i), V: strings.Repeat("x", 50), Type: entryTypePut})
	}
	entries[40] = ssTableEntry{K: "key-040", Type: entryTypeDelete}
	if err := writeBlockBasedTable(f, entries); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(contents) != 4*sstableBlockSize { // one index page and three data pages
		t.Fatalf("table size = %d, want %d", len(contents), 4*sstableBlockSize)
	}
	if contents[0] != 3 || string(contents[64:71]) != "key-000" || string(contents[128:135]) != "key-031" || string(contents[192:199]) != "key-062" {
		t.Fatal("index block does not contain the expected first keys")
	}
	if contents[8] != 0 || binary.LittleEndian.Uint64(contents[sstableBlockSize:sstableBlockSize+8]) == 0 || contents[sstableBlockSize+8] != 31 {
		t.Fatal("data block header does not contain the expected CRC and entry count")
	}

	table, err := openBlockBasedTable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer table.close()
	if len(table.index) < 2 {
		t.Fatalf("expected multiple data blocks, got %d", len(table.index))
	}
	for _, key := range []string{"key-000", "key-039", "key-064"} {
		entry, ok, err := table.get(key)
		if err != nil || !ok || entry.Type != entryTypePut {
			t.Fatalf("get(%q) = (%+v, %t, %v)", key, entry, ok, err)
		}
	}
	entry, ok, err := table.get("key-040")
	if err != nil || !ok || entry.Type != entryTypeDelete {
		t.Fatalf("get tombstone = (%+v, %t, %v)", entry, ok, err)
	}
	if _, ok, err := table.get("key-065"); err != nil || ok {
		t.Fatalf("missing key lookup = (found=%t, err=%v)", ok, err)
	}
}

func TestBlockBasedTableDetectsDataBlockCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.db")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeBlockBasedTable(f, []ssTableEntry{{K: "key", V: "value", Type: entryTypePut}}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	contents := mustReadFile(t, path)
	contents[sstableBlockSize+sstableDataHeaderSize] ^= 0xff
	if err := os.WriteFile(path, contents, 0o644); err != nil {
		t.Fatal(err)
	}
	table, err := openBlockBasedTable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer table.close()
	if _, _, err := table.get("key"); err == nil {
		t.Fatal("expected corrupted block CRC to be rejected")
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

func TestBlockBasedTableRejectsInvalidFooter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.db")
	if err := os.WriteFile(path, []byte("not an sstable"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openBlockBasedTable(path); err == nil {
		t.Fatal("expected invalid SSTable footer to be rejected")
	}
}
