package storage

import "testing"

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
