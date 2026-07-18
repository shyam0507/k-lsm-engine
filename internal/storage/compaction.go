package storage

import (
	"bufio"
	"container/heap"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type sstableReader struct {
	file    *os.File
	scanner *bufio.Scanner
	entry   ssTableEntry
	hasNext bool
}

func newSSTableReader(path string) (*sstableReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	r := &sstableReader{
		file:    f,
		scanner: bufio.NewScanner(f),
	}
	return r, nil
}

func (r *sstableReader) advance() error {
	for r.scanner.Scan() {
		line := strings.TrimSpace(r.scanner.Text())
		if line == "" {
			continue
		}

		var entry ssTableEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return err
		}

		r.entry = entry
		r.hasNext = true
		return nil
	}

	if err := r.scanner.Err(); err != nil {
		return err
	}

	r.hasNext = false
	return nil
}

func (r *sstableReader) close() error {
	return r.file.Close()
}

type sstableHeapItem struct {
	key        string
	entry      storageEntry
	tableIndex int
	level      int // stores the level of the table in lsm tree
}

type sstableHeap []sstableHeapItem

func (h sstableHeap) Len() int { return len(h) }
func (h sstableHeap) Less(i, j int) bool {
	if h[i].key != h[j].key {
		return h[i].key < h[j].key
	}

	if h[i].level != h[j].level {
		return h[i].level < h[j].level
	}

	return h[i].tableIndex > h[j].tableIndex
}
func (h sstableHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *sstableHeap) Push(x any)   { *h = append(*h, x.(sstableHeapItem)) }
func (h *sstableHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// by default compact level 0 and level 1
// compact and store to level 1
func (sst *sstableStore) compactSSTables() error {
	l0Tables := sst.getLevelSSTables(0)
	if len(l0Tables) < 2 {
		return nil
	}

	l1Tables := sst.getLevelSSTables(1)
	readers := make([]*sstableReader, 0, len(l0Tables)+len(l1Tables))
	h := &sstableHeap{}

	closeReaders := func() {
		for _, reader := range readers {
			if reader != nil {
				reader.close()
			}
		}
	}

	addTables := func(level int, tables []string) error {
		for _, table := range tables {
			r, err := newSSTableReader(filepath.Join(sst.dir, fmt.Sprintf("l%d", level), table))
			if err != nil {
				return err
			}

			if err := r.advance(); err != nil {
				r.close()
				return err
			}

			if !r.hasNext {
				r.close()
				continue
			}

			readers = append(readers, r)
			heap.Push(h, sstableHeapItem{
				key:        r.entry.K,
				entry:      storageEntry{Type: r.entry.Type, Value: r.entry.V},
				tableIndex: len(readers) - 1,
				level:      level,
			})
		}
		return nil
	}

	if err := addTables(0, l0Tables); err != nil {
		closeReaders()
		return err
	}
	if err := addTables(1, l1Tables); err != nil {
		closeReaders()
		return err
	}

	heap.Init(h)

	newTables := make([]string, 0)
	chunk := make(map[string]storageEntry, FLUSH_THRESHOLD)
	chunkCount := 0
	var currentKey string
	first := true

	nextL1Index := len(l1Tables) + 1
	flushChunk := func() error {
		if chunkCount == 0 {
			return nil
		}

		newTableName := fmt.Sprintf("%s%d%s", ssTablePrefix, nextL1Index, ssTableExt)
		nextL1Index++
		newFilePath := filepath.Join(sst.dir, fmt.Sprintf("l%d", 1), newTableName)
		if err := sst.writeSSTableFile(newFilePath, chunk); err != nil {
			return err
		}

		newTables = append(newTables, newTableName)
		chunk = make(map[string]storageEntry, FLUSH_THRESHOLD)
		chunkCount = 0
		return nil
	}

	for h.Len() > 0 {
		item := heap.Pop(h).(sstableHeapItem)

		if first || item.key != currentKey {
			if chunkCount == FLUSH_THRESHOLD {
				if err := flushChunk(); err != nil {
					closeReaders()
					return err
				}
			}

			chunk[item.key] = item.entry
			chunkCount++
			currentKey = item.key
			first = false
		}

		r := readers[item.tableIndex]
		if err := r.advance(); err != nil {
			closeReaders()
			return err
		}

		if r.hasNext {
			heap.Push(h, sstableHeapItem{
				key:        r.entry.K,
				entry:      storageEntry{Type: r.entry.Type, Value: r.entry.V},
				tableIndex: item.tableIndex,
				level:      item.level,
			})
		}
	}

	if err := flushChunk(); err != nil {
		closeReaders()
		return err
	}

	closeReaders()

	oldTablesLevel0 := append([]string(nil), l0Tables...)
	oldTablesLevel1 := append([]string(nil), l1Tables...)

	levels := make([]sstableLevel, 0, len(sst.levels))
	for _, existing := range sst.levels {
		if existing.level == 0 || existing.level == 1 {
			continue
		}
		levels = append(levels, existing)
	}

	levels = append(levels, sstableLevel{level: 0, tables: nil})
	levels = append(levels, sstableLevel{level: 1, tables: newTables})
	sort.Slice(levels, func(i, j int) bool {
		return levels[i].level < levels[j].level
	})

	if err := sst.rewriteManifestFromLevels(levels); err != nil {
		return err
	}

	sst.levels = levels

	for _, table := range oldTablesLevel0 {
		if err := os.Remove(filepath.Join(sst.dir, "l0", table)); err != nil && !os.IsNotExist(err) {
			slog.Error("Failed to remove old SSTable from level 0 after compaction", "table", table, "error", err)
		}
	}

	for _, table := range oldTablesLevel1 {
		if err := os.Remove(filepath.Join(sst.dir, "l1", table)); err != nil && !os.IsNotExist(err) {
			slog.Error("Failed to remove old SSTable from level 1 after compaction", "table", table, "error", err)
		}
	}

	slog.Info("SSTable compaction completed", "new_tables", len(newTables), "merged_entries", len(newTables)*FLUSH_THRESHOLD)
	return nil
}
