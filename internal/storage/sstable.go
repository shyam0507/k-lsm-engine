package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
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
	Type entryType `json:"type"`
}

type sstableLevel struct {
	tables []string
	level  int
}
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

	return &sstableStore{
		levels:       levels,
		dir:          dir,
		manifestPath: filepath.Join(dir, MANIFEST_FILE_NAME),
	}
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
			if !os.IsNotExist(statErr) {
				slog.Warn("Skipping unreadable SSTable while rebuilding level state", "level", levelEntry.level, "table", table, "path", tablePath, "error", statErr)
			}
		}
		pruned = append(pruned, sstableLevel{level: levelEntry.level, tables: prunedTables})
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

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		switch {
		case regexp.MustCompile(`^\[L[0-9]+\]$`).MatchString(line):
			if currentLevel >= 0 {
				levels = append(levels, sstableLevel{
					tables: tables,
					level:  currentLevel,
				})
			}

			idx, err := strconv.Atoi(strings.TrimPrefix(strings.TrimSuffix(line, "]"), "[L"))
			if err != nil {
				slog.Error("Invalid manifest level header", "line", line, "err", err)
				return levels, err
			}
			currentLevel = idx
			tables = nil
			if currentLevel > upToLevel {
				break
			}
		default:
			if currentLevel < 0 {
				continue
			}
			tables = append(tables, line)
		}
	}
	if err := scanner.Err(); err != nil {
		slog.Error("Error while getting data", "err", err)
		return levels, err
	}

	if currentLevel >= 0 {
		levels = append(levels, sstableLevel{
			tables: tables,
			level:  currentLevel,
		})
	}

	return levels, nil
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
	sst.mu.Lock()
	defer sst.mu.Unlock()

	slog.Info("SaveSSTable called")
	sstFileName := sst.getNewSSTableName(0)
	filePath := filepath.Join(sst.dir, "l0", sstFileName)

	if err := sst.writeSSTableFile(filePath, m); err != nil {
		slog.Error("Failed to write SSTable file", "file", filePath, "error", err)
		return err
	}

	slog.Info("SSTable file written in JSON Lines format and dir synched", "file", filePath)

	/*
		Now modify the manifest file
	*/
	if err := sst.appendTableToManifestLocked(sstFileName, 0); err != nil {
		if removeErr := os.Remove(filePath); removeErr != nil && !os.IsNotExist(removeErr) {
			slog.Warn("Failed to remove unpublished SSTable", "file", filePath, "error", removeErr)
		}
		slog.Error("Failed to append new table to manifest", "file", sst.manifestPath, "error", err)
		return err
	}

	slog.Info("SSTable written to disk with name", "name", sstFileName)
	return nil
}

func (sst *sstableStore) writeSSTableFile(filePath string, m map[string]storageEntry) error {
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
		}
	}()
	if err := f.Chmod(0644); err != nil {
		return err
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	for _, k := range keys {
		v := m[k]
		line, err := json.Marshal(ssTableEntry{K: k, V: v.Value, Type: v.Type})
		if err != nil {
			return err
		}
		if _, err := f.Write(append(line, '\n')); err != nil {
			return err
		}
	}

	if err := f.Sync(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		slog.Error("Failed to close new sstable file after write", "file", filePath, "error", err)
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
			if _, err := mf.WriteString(table + "\n"); err != nil {
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
		for i := len(levelTables) - 1; i >= 0; i-- {
			v := levelTables[i]
			f, err := os.Open(filepath.Join(sst.dir, fmt.Sprintf("l%d", level), v))
			if err != nil {
				slog.Error("Error while reading the ss table", "file", v, "error", err)
				continue
			}

			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := scanner.Text()
				if line == "" {
					continue
				}

				var entry ssTableEntry
				if err := json.Unmarshal([]byte(line), &entry); err != nil {
					f.Close()
					log.Fatalf("error during reading sstable: %s", err)
				}

				if key == entry.K {
					if entry.Type == "" {
						entry.Type = entryTypePut
					}

					f.Close()
					return storageEntry{
						Type:  entry.Type,
						Value: entry.V,
					}, true
				}
			}

			if err := scanner.Err(); err != nil {
				f.Close()
				log.Fatalf("error during reading sstable: %s", err)
			}

			f.Close()
		}
	}
	return storageEntry{}, false
}
