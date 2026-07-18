package storage

import (
	"bufio"
	"container/heap"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
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

	if h[i].level == h[j].level {
		return h[i].tableIndex < h[j].tableIndex
	}

	return h[i].level < h[j].level

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
func (sst *sstableStore) compactSSTables() error {
	if len(sst.getLevelSSTables(0)) < 2 {
		return nil
	}

	readers := make([]*sstableReader, len(sst.getLevelSSTables(0)))
	h := &sstableHeap{}
	for _, level := range sst.levels {
		for i, table := range level.tables {
			//TODO change to read filepath based upon the dir
			r, err := newSSTableReader(filepath.Join(sst.dir, table))
			if err != nil {
				return err
			}
			readers[i] = r

			if err := r.advance(); err != nil {
				for j := 0; j <= i; j++ {
					if readers[j] != nil {
						readers[j].close()
					}
				}
				return err
			}

			if r.hasNext {
				heap.Push(h, sstableHeapItem{
					key:        r.entry.K,
					entry:      storageEntry{Type: r.entry.Type, Value: r.entry.V},
					tableIndex: i,
					level:      sst.levels[0].level,
				})
			}
		}
	}

	heap.Init(h)

	var newTables []string
	chunk := make(map[string]storageEntry, FLUSH_THRESHOLD)
	chunkCount := 0
	var currentKey string
	first := true

	flushChunk := func() error {
		if chunkCount == 0 {
			return nil
		}

		newTableName := sst.getSSTableName(0)
		newFilePath := filepath.Join(sst.dir, newTableName)
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
			for _, reader := range readers {
				if reader != nil {
					reader.close()
				}
			}
			return err
		}

		if r.hasNext {
			heap.Push(h, sstableHeapItem{
				key:        r.entry.K,
				entry:      storageEntry{Type: r.entry.Type, Value: r.entry.V},
				tableIndex: item.tableIndex,
			})
		}
	}

	if err := flushChunk(); err != nil {
		for _, reader := range readers {
			if reader != nil {
				reader.close()
			}
		}
		return err
	}

	for _, reader := range readers {
		if reader != nil {
			reader.close()
		}
	}

	oldTables := append([]string(nil), sst.tables...)

	// Preserve manifest oldest-to-newest ordering, but keep in-memory table list newest-first.
	sst.tables = make([]string, len(newTables))
	for i, table := range newTables {
		sst.tables[len(newTables)-1-i] = table
	}

	if err := sst.rewriteManifest(newTables); err != nil {
		return err
	}

	for _, table := range oldTables {
		if err := os.Remove(filepath.Join(sst.dir, table)); err != nil && !os.IsNotExist(err) {
			slog.Error("Failed to remove old SSTable after compaction", "table", table, "error", err)
		}
	}

	slog.Info("SSTable compaction completed", "new_tables", len(newTables), "merged_entries", len(newTables)*FLUSH_THRESHOLD)
	return nil
}
