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
	path    string
}

func newSSTableReader(path string) (*sstableReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	r := &sstableReader{
		file:    f,
		scanner: bufio.NewScanner(f),
		path:    path,
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
			return fmt.Errorf("decode SSTable entry in %s: %w", r.path, err)
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

func (sst *sstableStore) pruneLevelTables(level int, tables []string) []string {
	pruned := make([]string, 0, len(tables))
	for _, table := range tables {
		tablePath := filepath.Join(sst.dir, fmt.Sprintf("l%d", level), table)
		_, statErr := os.Stat(tablePath)
		if statErr == nil {
			pruned = append(pruned, table)
			continue
		}

		if !os.IsNotExist(statErr) {
			slog.Warn("Skipping unreadable SSTable while pruning level state", "level", level, "table", table, "path", tablePath, "error", statErr)
			continue
		}

		slog.Warn("Removing missing SSTable from manifest state", "level", level, "table", table, "path", tablePath)
	}

	return pruned
}

func getTablesForLevel(levels []sstableLevel, level int) []string {
	for _, levelEntry := range levels {
		if levelEntry.level == level {
			return levelEntry.tables
		}
	}
	return nil
}

// by default compact level 0 and level 1
// compact and store to level 1
func (sst *sstableStore) compactSSTables() error {
	sst.mu.Lock()
	defer sst.mu.Unlock()

	prunedLevels := make([]sstableLevel, 0, len(sst.levels))
	for _, existing := range sst.levels {
		switch existing.level {
		case 0:
			prunedLevels = append(prunedLevels, sstableLevel{level: 0, tables: sst.pruneLevelTables(0, existing.tables)})
		case 1:
			prunedLevels = append(prunedLevels, sstableLevel{level: 1, tables: sst.pruneLevelTables(1, existing.tables)})
		default:
			prunedLevels = append(prunedLevels, existing)
		}
	}

	l0Tables := getTablesForLevel(prunedLevels, 0)
	if len(l0Tables) < 2 {
		if err := sst.rewriteManifestFromLevels(prunedLevels); err != nil {
			return err
		}
		sst.levels = prunedLevels
		return nil
	}

	l1Tables := getTablesForLevel(prunedLevels, 1)
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
			tablePath := filepath.Join(sst.dir, fmt.Sprintf("l%d", level), table)
			r, err := newSSTableReader(tablePath)
			if err != nil {
				if os.IsNotExist(err) {
					slog.Warn("Skipping missing SSTable during compaction", "level", level, "table", table, "path", tablePath)
					continue
				}
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
	published := false
	defer func() {
		if published {
			return
		}
		for _, table := range newTables {
			if err := os.Remove(filepath.Join(sst.dir, "l1", table)); err != nil && !os.IsNotExist(err) {
				slog.Warn("Failed to remove unpublished compacted SSTable", "table", table, "error", err)
			}
		}
	}()
	chunk := make(map[string]storageEntry, FLUSH_THRESHOLD)
	chunkCount := 0
	mergedEntries := 0
	var currentKey string
	first := true

	nextL1Index := getNextSSTableIndex(l1Tables)
	flushChunk := func() error {
		if chunkCount == 0 {
			return nil
		}

		var newTableName, newFilePath string
		for {
			newTableName = fmt.Sprintf("%s%d%s", ssTablePrefix, nextL1Index, ssTableExt)
			nextL1Index++
			newFilePath = filepath.Join(sst.dir, "l1", newTableName)
			_, err := os.Stat(newFilePath)
			if os.IsNotExist(err) {
				break
			}
			if err != nil {
				return err
			}
		}
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
			mergedEntries++
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

	levels := make([]sstableLevel, 0, len(prunedLevels))
	for _, existing := range prunedLevels {
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
	published = true

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

	slog.Info("SSTable compaction completed", "new_tables", len(newTables), "merged_entries", mergedEntries)
	return nil
}
