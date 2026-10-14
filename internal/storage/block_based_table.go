package storage

import (
	"bytes"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"sort"
)

// The on-disk table format is page-aligned: one 4 KiB index page followed by
// up to 63 4 KiB data pages
const (
	sstableBlockSize        = 4 * 1024
	sstableIndexCapacity    = 63
	sstableDataBlockEntries = 31
	sstableKeyValueSize     = 64
	sstableDataHeaderSize   = 128
	sstableMaxEntries       = sstableIndexCapacity * sstableDataBlockEntries
)

var sstableCRCTable = crc64.MakeTable(crc64.ECMA)

type blockBasedTable struct {
	file  *os.File
	index []string // first key for each data block, in sorted order
}

func writeBlockBasedTable(f *os.File, entries []ssTableEntry) error {
	if len(entries) > sstableMaxEntries {
		return fmt.Errorf("SSTable has %d entries, maximum is %d", len(entries), sstableMaxEntries)
	}
	for i := 1; i < len(entries); i++ {
		// the keys will be unique in a sstable
		if entries[i-1].K >= entries[i].K {
			return fmt.Errorf("SSTable entries are not strictly sorted")
		}
	}
	blockCount := (len(entries) + sstableDataBlockEntries - 1) / sstableDataBlockEntries
	indexBlock := make([]byte, sstableBlockSize)
	indexBlock[0] = byte(blockCount)

	for blockID := 0; blockID < blockCount; blockID++ {
		start := blockID * sstableDataBlockEntries
		end := min(start+sstableDataBlockEntries, len(entries))
		if err := writePaddedString(indexBlock[64+blockID*sstableKeyValueSize:], entries[start].K, "index key"); err != nil {
			return err
		}
		dataBlock, err := encodeDataBlock(entries[start:end])
		if err != nil {
			return err
		}
		if _, err := f.WriteAt(dataBlock, int64((blockID+1)*sstableBlockSize)); err != nil {
			return err
		}
	}
	_, err := f.WriteAt(indexBlock, 0)
	return err
}

func encodeDataBlock(entries []ssTableEntry) ([]byte, error) {
	if len(entries) > sstableDataBlockEntries {
		return nil, fmt.Errorf("data block has %d entries, maximum is %d", len(entries), sstableDataBlockEntries)
	}
	block := make([]byte, sstableBlockSize)
	block[8] = byte(len(entries))
	for i, entry := range entries {
		if i > 0 && entries[i-1].K >= entry.K {
			return nil, fmt.Errorf("SSTable entries are not strictly sorted")
		}
		offset := sstableDataHeaderSize + i*2*sstableKeyValueSize
		if err := writePaddedString(block[offset:offset+sstableKeyValueSize], entry.K, "key"); err != nil {
			return nil, err
		}
		//! for delete value is empty
		if entry.Type == entryTypePut {
			if err := writePaddedString(block[offset+sstableKeyValueSize:offset+2*sstableKeyValueSize], entry.V, "value"); err != nil {
				return nil, err
			}
		} else if entry.Type != entryTypeDelete {
			return nil, fmt.Errorf("invalid SSTable entry type %q", entry.Type)
		}
	}
	crc := crc64.Checksum(block[8:], sstableCRCTable)
	for i := 0; i < 8; i++ {
		block[i] = byte(crc >> (8 * i))
	}
	return block, nil
}

func writePaddedString(dst []byte, value, field string) error {
	if value == "" {
		return fmt.Errorf("SSTable %s cannot be empty", field)
	}
	if len(value) > len(dst) {
		return fmt.Errorf("SSTable %s exceeds %d bytes", field, len(dst))
	}
	if bytes.IndexByte([]byte(value), 0) >= 0 {
		return fmt.Errorf("SSTable %s contains a NUL byte", field)
	}
	copy(dst, value)
	return nil
}

func openBlockBasedTable(path string) (*blockBasedTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	closeOnError := func(err error) (*blockBasedTable, error) { _ = f.Close(); return nil, err }
	info, err := f.Stat()
	if err != nil {
		return closeOnError(err)
	}
	if info.Size() < sstableBlockSize || info.Size()%sstableBlockSize != 0 {
		return closeOnError(fmt.Errorf("SSTable %s does not contain page-aligned blocks", path))
	}
	indexBlock := make([]byte, sstableBlockSize)
	if _, err := f.ReadAt(indexBlock, 0); err != nil && err != io.EOF {
		return closeOnError(err)
	}
	blockCount := int(indexBlock[0])
	if blockCount > sstableIndexCapacity || int64(blockCount+1)*sstableBlockSize != info.Size() {
		return closeOnError(fmt.Errorf("SSTable %s has an invalid block count", path))
	}
	index := make([]string, blockCount)
	for i := range index {
		key := readPaddedString(indexBlock[64+i*sstableKeyValueSize : 64+(i+1)*sstableKeyValueSize])
		if key == "" || (i > 0 && index[i-1] >= key) {
			return closeOnError(fmt.Errorf("SSTable %s has an invalid index", path))
		}
		index[i] = key
	}
	return &blockBasedTable{file: f, index: index}, nil
}

func (t *blockBasedTable) close() error { return t.file.Close() }

func (t *blockBasedTable) readBlock(i int) ([]ssTableEntry, error) {
	if i < 0 || i >= len(t.index) {
		return nil, fmt.Errorf("SSTable block index out of range")
	}
	block := make([]byte, sstableBlockSize)
	if _, err := t.file.ReadAt(block, int64((i+1)*sstableBlockSize)); err != nil && err != io.EOF {
		return nil, err
	}
	storedCRC := uint64(0)
	for j := 0; j < 8; j++ {
		storedCRC |= uint64(block[j]) << (8 * j)
	}
	if actualCRC := crc64.Checksum(block[8:], sstableCRCTable); storedCRC != actualCRC {
		return nil, fmt.Errorf("SSTable block %d CRC mismatch", i)
	}
	count := int(block[8])
	if count > sstableDataBlockEntries {
		return nil, fmt.Errorf("SSTable block %d has invalid entry count", i)
	}
	entries := make([]ssTableEntry, 0, count)
	for j := 0; j < count; j++ {
		offset := sstableDataHeaderSize + j*2*sstableKeyValueSize
		key := readPaddedString(block[offset : offset+sstableKeyValueSize])
		valueBytes := block[offset+sstableKeyValueSize : offset+2*sstableKeyValueSize]
		if key == "" || (j > 0 && entries[j-1].K >= key) {
			return nil, fmt.Errorf("SSTable block %d has unsorted entries", i)
		}
		entry := ssTableEntry{K: key, Type: entryTypeDelete}
		if !allZero(valueBytes) {
			entry.Type = entryTypePut
			entry.V = readPaddedString(valueBytes)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (t *blockBasedTable) get(key string) (storageEntry, bool, error) {
	blockID := sort.Search(len(t.index), func(i int) bool { return t.index[i] > key }) - 1
	if blockID < 0 {
		return storageEntry{}, false, nil
	}
	entries, err := t.readBlock(blockID)
	if err != nil {
		return storageEntry{}, false, err
	}
	entryID := sort.Search(len(entries), func(i int) bool { return entries[i].K >= key })
	if entryID == len(entries) || entries[entryID].K != key {
		return storageEntry{}, false, nil
	}
	entry := entries[entryID]
	return storageEntry{Type: entry.Type, Value: entry.V}, true, nil
}

func readPaddedString(src []byte) string {
	return string(bytes.TrimRight(src, "\x00"))
}

func allZero(src []byte) bool {
	for _, b := range src {
		if b != 0 {
			return false
		}
	}
	return true
}
