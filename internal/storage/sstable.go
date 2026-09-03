package storage

import (
	"bufio"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	MANIFEST_FILE_NAME = "manifest.db"
)

// ssTableEntry represents a key-value pair in the table
type ssTableEntry struct {
	K    string    `json:"k"`
	V    string    `json:"v"`
	Type entryType `json:"type"` //not peristed on disk used in memory only
}

// struct used to hold information of each level of the sstable, every level will have this struct
type sstableLevel struct {
	tables       []string
	bloomFilters map[string]bloomFilter
	keyRanges    map[string]sstableKeyRange // populated for L1 tables
	level        int
}

// sstableKeyRange is inclusive. L1 tables are sorted and non-overlapping, so
// the range lets reads avoid opening tables that cannot contain a key.
type sstableKeyRange struct {
	min string
	max string
}

// represents the entire sstable including all the levels
type sstableStore struct {
	dir          string
	manifestPath string
	levels       []sstableLevel
	mu           sync.RWMutex
}

func newSSTableStore(dir string) *sstableStore {
	levels, err := getAllSSTables(dir, MAX_LEVELS)

	if err != nil {
		log.Fatal("Error while parsing/loading sstable", err)
	}

	levels = pruneLevelsFromDisk(dir, levels)
	levels = loadBloomFilters(dir, levels)

	return &sstableStore{
		levels:       levels,
		dir:          dir,
		manifestPath: filepath.Join(dir, MANIFEST_FILE_NAME),
	}
}

// load all the bloom filters into memory for the sstable
func loadBloomFilters(dir string, levels []sstableLevel) []sstableLevel {
	for i := range levels {
		filters := make(map[string]bloomFilter, len(levels[i].tables))
		for _, table := range levels[i].tables {
			tablePath := filepath.Join(dir, fmt.Sprintf("l%d", levels[i].level), table)
			filter, err := readBloomFilterSidecar(tablePath)
			if err != nil {
				slog.Warn("Bloom filter unavailable; falling back to SSTable read", "level", levels[i].level, "table", table, "error", err)
				continue
			}
			filters[table] = filter
		}
		levels[i].bloomFilters = filters
	}
	return levels
}

// after flushing a sstable update the bloom filter in memory
func (sst *sstableStore) registerBloomFilter(level int, table string) {
	tablePath := filepath.Join(sst.dir, fmt.Sprintf("l%d", level), table)
	filter, err := readBloomFilterSidecar(tablePath)
	if err != nil {
		slog.Warn("Bloom filter unavailable; falling back to SSTable read", "level", level, "table", table, "error", err)
		return
	}
	for i := range sst.levels {
		if sst.levels[i].level != level {
			continue
		}
		if sst.levels[i].bloomFilters == nil {
			sst.levels[i].bloomFilters = make(map[string]bloomFilter)
		}
		sst.levels[i].bloomFilters[table] = filter
		return
	}
}

func removeSSTableAndBloom(tablePath string) error {
	if err := os.Remove(tablePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(bloomSidecarPath(tablePath)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func pruneLevelsFromDisk(dir string, levels []sstableLevel) []sstableLevel {
	pruned := make([]sstableLevel, 0, len(levels))
	for _, levelEntry := range levels {
		prunedTables := make([]string, 0, len(levelEntry.tables))
		for _, table := range levelEntry.tables {
			tablePath := filepath.Join(dir, fmt.Sprintf("l%d", levelEntry.level), table)
			_, statErr := os.Stat(tablePath)
			if statErr == nil {
				prunedTables = append(prunedTables, table)
				continue
			}
			if os.IsNotExist(statErr) {
				if removeErr := os.Remove(bloomSidecarPath(tablePath)); removeErr != nil && !os.IsNotExist(removeErr) {
					slog.Warn("Failed to remove orphaned bloom filter", "level", levelEntry.level, "table", table, "error", removeErr)
				}
				continue
			}
			slog.Warn("Skipping unreadable SSTable while rebuilding level state", "level", levelEntry.level, "table", table, "path", tablePath, "error", statErr)
		}
		prunedRanges := make(map[string]sstableKeyRange, len(prunedTables))
		for _, table := range prunedTables {
			if keyRange, ok := levelEntry.keyRanges[table]; ok {
				prunedRanges[table] = keyRange
			}
		}
		pruned = append(pruned, sstableLevel{level: levelEntry.level, tables: prunedTables, keyRanges: prunedRanges})
	}
	return pruned
}

// return the sstable slice of a level
func (sst *sstableStore) getLevelSSTables(level int) []string {
	sst.mu.RLock()
	defer sst.mu.RUnlock()
	return sst.getLevelSSTablesLocked(level)
}

func (sst *sstableStore) getLevelSSTablesLocked(level int) []string {
	for _, v := range sst.levels {
		if v.level == level {
			return append([]string(nil), v.tables...)
		}
	}
	return nil
}

func l1TableMayContain(key string, keyRange sstableKeyRange, hasRange bool) bool {
	return !hasRange || (key >= keyRange.min && key <= keyRange.max)
}

// getAllSSTables reads the manifest file from the SSTable directory and returns
// loaded table names in reverse order plus the current counter.
func getAllSSTables(dir string, upToLevel int) ([]sstableLevel, error) {
	levels := make([]sstableLevel, 0)

	f, err := os.Open(filepath.Join(dir, MANIFEST_FILE_NAME))
	if err != nil {
		if os.IsNotExist(err) {
			return levels, nil
		}
		return levels, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var currentLevel = -1
	var tables []string
	var keyRanges map[string]sstableKeyRange

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		switch {
		case regexp.MustCompile(`^\[L[0-9]+\]$`).MatchString(line):
			if currentLevel >= 0 {
				levels = append(levels, sstableLevel{
					tables:    tables,
					keyRanges: keyRanges,
					level:     currentLevel,
				})
			}

			idx, err := strconv.Atoi(strings.TrimPrefix(strings.TrimSuffix(line, "]"), "[L"))
			if err != nil {
				slog.Error("Invalid manifest level header", "line", line, "err", err)
				return levels, err
			}
			currentLevel = idx
			tables = nil
			keyRanges = make(map[string]sstableKeyRange)
			if currentLevel > upToLevel {
				break
			}
		default:
			if currentLevel < 0 {
				continue
			}
			table, keyRange, hasRange, err := parseManifestTable(line, currentLevel)
			if err != nil {
				return levels, err
			}
			tables = append(tables, table)
			if hasRange {
				keyRanges[table] = keyRange
			}
		}
	}
	if err := scanner.Err(); err != nil {
		slog.Error("Error while getting data", "err", err)
		return levels, err
	}

	if currentLevel >= 0 {
		levels = append(levels, sstableLevel{
			tables:    tables,
			keyRanges: keyRanges,
			level:     currentLevel,
		})
	}

	return levels, nil
}

// L1 entries are encoded as: <table>\t<quoted-min-key>\t<quoted-max-key>.
// Quoting keeps arbitrary string keys, including whitespace, unambiguous.
// Entries without bounds are accepted for manifests written by older versions.
func parseManifestTable(line string, level int) (string, sstableKeyRange, bool, error) {
	if level != 1 || !strings.Contains(line, "\t") {
		return line, sstableKeyRange{}, false, nil
	}

	parts := strings.Split(line, "\t")
	if len(parts) != 3 || parts[0] == "" {
		return "", sstableKeyRange{}, false, fmt.Errorf("invalid L1 manifest entry %q", line)
	}
	minKey, err := strconv.Unquote(parts[1])
	if err != nil {
		return "", sstableKeyRange{}, false, fmt.Errorf("decode L1 minimum key for %s: %w", parts[0], err)
	}
	maxKey, err := strconv.Unquote(parts[2])
	if err != nil {
		return "", sstableKeyRange{}, false, fmt.Errorf("decode L1 maximum key for %s: %w", parts[0], err)
	}
	if minKey > maxKey {
		return "", sstableKeyRange{}, false, fmt.Errorf("invalid L1 key range for %s: %q > %q", parts[0], minKey, maxKey)
	}
	return parts[0], sstableKeyRange{min: minKey, max: maxKey}, true, nil
}

func getSSTableIndex(table string) int {
	if !strings.HasPrefix(table, ssTablePrefix) || !strings.HasSuffix(table, ssTableExt) {
		return 0
	}

	numPart := strings.TrimSuffix(strings.TrimPrefix(table, ssTablePrefix), ssTableExt)
	idx, err := strconv.Atoi(numPart)
	if err != nil || idx <= 0 {
		return 0
	}

	return idx
}

func getNextSSTableIndex(tables []string) int {
	maxIdx := 0
	for _, table := range tables {
		if idx := getSSTableIndex(table); idx > maxIdx {
			maxIdx = idx
		}
	}
	return maxIdx + 1
}

func (sst *sstableStore) getNewSSTableName(level int) string {
	count := 1
	for _, v := range sst.levels {
		if v.level == level {
			count = getNextSSTableIndex(v.tables)
			break
		}
	}
	return fmt.Sprintf("%s%d%s", ssTablePrefix, count, ssTableExt)
}

// saves ss table to level 0
func (sst *sstableStore) saveLevel0SSTable(m map[string]storageEntry) error {
	return sst.saveLevel0SSTableWithWriter(func(filePath string) error {
		return sst.writeSSTableFile(filePath, m)
	})
}

// saveLevel0SSTableEntries saves entries that are already sorted by key.
func (sst *sstableStore) saveLevel0SSTableEntries(entries []ssTableEntry) error {
	return sst.saveLevel0SSTableWithWriter(func(filePath string) error {
		return sst.writeSortedSSTableFile(filePath, entries)
	})
}

// save l0file and modify manifest
func (sst *sstableStore) saveLevel0SSTableWithWriter(writeTable func(filePath string) error) error {
	sst.mu.Lock()
	defer sst.mu.Unlock()

	slog.Info("SaveSSTable called")
	sstFileName := sst.getNewSSTableName(0)
	filePath := filepath.Join(sst.dir, "l0", sstFileName)

	if err := writeTable(filePath); err != nil {
		slog.Error("Failed to write SSTable file", "file", filePath, "error", err)
		return err
	}

	slog.Info("Block-based SSTable file written and directory synched", "file", filePath)

	/*
		Now modify the manifest file
	*/
	if err := sst.appendTableToManifestLocked(sstFileName, 0); err != nil {
		if removeErr := removeSSTableAndBloom(filePath); removeErr != nil {
			slog.Warn("Failed to remove unpublished SSTable and bloom filter", "file", filePath, "error", removeErr)
		}
		slog.Error("Failed to append new table to manifest", "file", sst.manifestPath, "error", err)
		return err
	}
	sst.registerBloomFilter(0, sstFileName)

	slog.Info("SSTable written to disk with name", "name", sstFileName)
	return nil
}

func (sst *sstableStore) writeSSTableFile(filePath string, m map[string]storageEntry) error {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	entries := make([]ssTableEntry, 0, len(keys))
	for _, k := range keys {
		v := m[k]
		entries = append(entries, ssTableEntry{K: k, V: v.Value, Type: v.Type})
	}
	return sst.writeSortedSSTableFile(filePath, entries)
}

// writeSortedSSTableFile writes entries in key order. Callers must preserve
// that ordering because the block index relies on sorted SSTable contents.
func (sst *sstableStore) writeSortedSSTableFile(filePath string, entries []ssTableEntry) error {
	dirPath := filepath.Dir(filePath)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return err
	}

	f, err := os.CreateTemp(dirPath, "."+filepath.Base(filePath)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := f.Name()
	committed := false
	defer func() {
		if !committed {
			_ = f.Close()
			_ = os.Remove(tempPath)
			_ = os.Remove(bloomSidecarPath(filePath))
		}
	}()
	if err := f.Chmod(0644); err != nil {
		return err
	}

	if err := writeBlockBasedTable(f, entries); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		slog.Error("Failed to close new sstable file after write", "file", filePath, "error", err)
		return err
	}
	if err := writeBloomFilterSidecar(filePath, bloomFilterForEntries(entries)); err != nil {
		return err
	}

	if err := os.Rename(tempPath, filePath); err != nil {
		return err
	}
	committed = true
	return syncParentDir(filePath)
}

func (sst *sstableStore) rewriteManifestFromLevels(levels []sstableLevel) error {
	sort.Slice(levels, func(i, j int) bool {
		return levels[i].level < levels[j].level
	})

	mf, err := os.CreateTemp(filepath.Dir(sst.manifestPath), "."+filepath.Base(sst.manifestPath)+".tmp-*")
	if err != nil {
		slog.Error("Failed to create temporary manifest", "file", sst.manifestPath, "error", err)
		return err
	}
	tempPath := mf.Name()
	committed := false
	defer func() {
		if !committed {
			_ = mf.Close()
			_ = os.Remove(tempPath)
		}
	}()
	if err := mf.Chmod(0644); err != nil {
		return err
	}

	for _, levelEntry := range levels {
		if _, err := mf.WriteString(fmt.Sprintf("[L%d]\n", levelEntry.level)); err != nil {
			slog.Error("Failed to write manifest header", "file", sst.manifestPath, "error", err)
			return err
		}

		for _, table := range levelEntry.tables {
			line := table
			if levelEntry.level == 1 {
				if keyRange, ok := levelEntry.keyRanges[table]; ok {
					line = fmt.Sprintf("%s\t%q\t%q", table, keyRange.min, keyRange.max)
				}
			}
			if _, err := mf.WriteString(line + "\n"); err != nil {
				slog.Error("Failed to write manifest table", "file", sst.manifestPath, "error", err)
				return err
			}
		}

		if _, err := mf.WriteString("\n"); err != nil {
			slog.Error("Failed to write manifest separator", "file", sst.manifestPath, "error", err)
			return err
		}
	}

	if err := mf.Sync(); err != nil {
		slog.Error("Failed to fsync the manifest file", "error", err)
		return err
	}

	if err := mf.Close(); err != nil {
		slog.Error("Failed to close manifest file after rewrite", "file", sst.manifestPath, "error", err)
		return err
	}

	if err := os.Rename(tempPath, sst.manifestPath); err != nil {
		slog.Error("Failed to publish manifest", "temp_file", tempPath, "file", sst.manifestPath, "error", err)
		return err
	}
	committed = true
	return syncParentDir(sst.manifestPath)
}

func (sst *sstableStore) appendTableToManifestLocked(table string, level int) error {
	levels, err := getAllSSTables(sst.dir, MAX_LEVELS)
	if err != nil {
		return err
	}

	inserted := false
	for i := range levels {
		if levels[i].level == level {
			levels[i].tables = append(levels[i].tables, table)
			inserted = true
			break
		}
	}

	if !inserted {
		levels = append(levels, sstableLevel{
			level:  level,
			tables: []string{table},
		})
	}

	if err := sst.rewriteManifestFromLevels(levels); err != nil {
		return err
	}

	for i := range sst.levels {
		if sst.levels[i].level == level {
			sst.levels[i].tables = append(sst.levels[i].tables, table)
			return nil
		}
	}

	sst.levels = append(sst.levels, sstableLevel{level: level, tables: []string{table}})
	sort.Slice(sst.levels, func(i, j int) bool {
		return sst.levels[i].level < sst.levels[j].level
	})

	return nil
}

func (sst *sstableStore) getKey(key string) (storageEntry, bool) {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	for level := 0; level < MAX_LEVELS; level++ {
		levelTables := sst.getLevelSSTablesLocked(level)
		var keyRanges map[string]sstableKeyRange
		var bloomFilters map[string]bloomFilter
		if level == 1 {
			for _, levelEntry := range sst.levels {
				if levelEntry.level == level {
					keyRanges = levelEntry.keyRanges
					break
				}
			}
		}
		for _, levelEntry := range sst.levels {
			if levelEntry.level == level {
				bloomFilters = levelEntry.bloomFilters
				break
			}
		}
		for i := len(levelTables) - 1; i >= 0; i-- {
			v := levelTables[i]
			if keyRange, ok := keyRanges[v]; !l1TableMayContain(key, keyRange, ok) {
				continue
			}
			if filter, ok := bloomFilters[v]; ok && !filter.found(key) {
				continue
			}
			tablePath := filepath.Join(sst.dir, fmt.Sprintf("l%d", level), v)
			table, err := openBlockBasedTable(tablePath)
			if err != nil {
				slog.Error("Error while opening block-based SSTable", "file", v, "error", err)
				continue
			}
			entry, found, err := table.get(key)
			closeErr := table.close()
			if err != nil {
				slog.Error("Error while reading block-based SSTable", "file", v, "error", err)
				continue
			}
			if closeErr != nil {
				slog.Warn("Error while closing block-based SSTable", "file", v, "error", closeErr)
			}
			if found {
				return entry, true
			}
		}
	}
	return storageEntry{}, false
}
