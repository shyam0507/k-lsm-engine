package storage

import "testing"

func TestSkipListEntriesAreSorted(t *testing.T) {
	list := newSkipList()
	list.add("charlie", storageEntry{Type: entryTypePut, Value: "3"})
	list.add("alpha", storageEntry{Type: entryTypePut, Value: "1"})
	list.add("bravo", storageEntry{Type: entryTypePut, Value: "2"})

	entries := list.entries()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	for i, key := range []string{"alpha", "bravo", "charlie"} {
		if entries[i].K != key {
			t.Fatalf("entry %d: expected key %q, got %q", i, key, entries[i].K)
		}
	}
}

func TestSkipListEntriesReflectOverwrite(t *testing.T) {
	list := newSkipList()
	list.add("key", storageEntry{Type: entryTypePut, Value: "old"})
	list.add("key", storageEntry{Type: entryTypePut, Value: "new"})

	entries := list.entries()
	if len(entries) != 1 || entries[0].V != "new" {
		t.Fatalf("expected one overwritten entry with value new, got %#v", entries)
	}
}
