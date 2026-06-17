package storage

import (
	"os"
	"testing"
)

func TestNewWALEntryCalculatesPayloadCRC(t *testing.T) {
	entry, err := newWALEntry("key", "value", entryTypePut)
	if err != nil {
		t.Fatalf("new wal entry: %v", err)
	}

	expected, err := calculateCRC(walPayload{
		Key:   "key",
		Type:  entryTypePut,
		Value: "value",
	})
	if err != nil {
		t.Fatalf("calculate crc: %v", err)
	}

	if entry.CRC != expected {
		t.Fatalf("expected crc %q, got %q", expected, entry.CRC)
	}
}

func TestDifferentPayloadsHaveDifferentCRCs(t *testing.T) {
	putCRC, err := calculateCRC(walPayload{
		Key:   "key",
		Type:  entryTypePut,
		Value: "value",
	})
	if err != nil {
		t.Fatalf("calculate put crc: %v", err)
	}

	deleteCRC, err := calculateCRC(walPayload{
		Key:  "key",
		Type: entryTypeDelete,
	})
	if err != nil {
		t.Fatalf("calculate delete crc: %v", err)
	}

	if putCRC == deleteCRC {
		t.Fatalf("expected different crcs for put and delete payloads, got %q", putCRC)
	}
}

func TestWALGetAllReadsAppendedEntries(t *testing.T) {
	w := newWAL(t.TempDir())

	putEntry, err := newWALEntry("key", "value", entryTypePut)
	if err != nil {
		t.Fatalf("new put wal entry: %v", err)
	}
	if err := w.append(putEntry); err != nil {
		t.Fatalf("append put entry: %v", err)
	}

	deleteEntry, err := newWALEntry("key", "", entryTypeDelete)
	if err != nil {
		t.Fatalf("new delete wal entry: %v", err)
	}
	if err := w.append(deleteEntry); err != nil {
		t.Fatalf("append delete entry: %v", err)
	}

	entries, err := w.getAll()
	if err != nil {
		t.Fatalf("get all wal entries: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 wal entries, got %d", len(entries))
	}
	if entries[0] != (walPayload{Key: "key", Type: entryTypePut, Value: "value"}) {
		t.Fatalf("unexpected first wal entry: %#v", entries[0])
	}
	if entries[1] != (walPayload{Key: "key", Type: entryTypeDelete}) {
		t.Fatalf("unexpected second wal entry: %#v", entries[1])
	}
}

func TestWALGetAllCreatesMissingFile(t *testing.T) {
	w := newWAL(t.TempDir())

	entries, err := w.getAll()
	if err != nil {
		t.Fatalf("get all missing wal entries: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no entries for missing wal, got %d", len(entries))
	}
}

func TestWALGetAllRejectsInvalidCRC(t *testing.T) {
	w := newWAL(t.TempDir())

	data := []byte(`{"crc":"invalid","key":"key","type":"PUT","value":"value"}` + "\n")
	if err := os.WriteFile(w.path, data, 0644); err != nil {
		t.Fatalf("write corrupted wal: %v", err)
	}

	if _, err := w.getAll(); err == nil {
		t.Fatal("expected invalid crc error")
	}
}
