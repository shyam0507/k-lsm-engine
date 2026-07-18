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
}

func newSSTableStore(dir string) *sstableStore {
	levels, err := getAllSSTables(dir, MAX_LEVELS)

	if err != nil {
		log.Fatal("Error while parsing/loading sstable", err)
	}

	return &sstableStore{
		levels:       levels,
		dir:          dir,
		manifestPath: filepath.Join(dir, MANIFEST_FILE_NAME),
	}
}

// return the sstable slice of a level
func (sst *sstableStore) getLevelSSTables(level int) []string {
	for _, v := range sst.levels {
		if v.level == level {
			return v.tables
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

func (sst *sstableStore) getSSTableName(level int) string {
	count := 1
	for _, v := range sst.levels {
		if v.level == level {
			count = 1 + len(v.tables)
			break
		}
	}
	return fmt.Sprintf("%s%d%s", ssTablePrefix, count, ssTableExt)
}

// saves ss table to level 0
func (sst *sstableStore) saveLevel0SSTable(m map[string]storageEntry) error {
	slog.Info("SaveSSTable called")
	sstFileName := sst.getSSTableName(0)
	filePath := filepath.Join(sst.dir, "l0", sstFileName)

	if err := sst.writeSSTableFile(filePath, m); err != nil {
		slog.Error("Failed to write SSTable file", "file", filePath, "error", err)
		return err
	}

	slog.Info("SSTable file written in JSON Lines format and dir synched", "file", filePath)

	/*
		Now modify the manifest file
	*/
	if err := sst.appendTableToManifest(sstFileName, 0); err != nil {
		slog.Error("Failed to append new table to manifest", "file", sst.manifestPath, "error", err)
		return err
	}

	slog.Info("SSTable written to disk with name", "name", sstFileName)
	return nil
}

func (sst *sstableStore) writeSSTableFile(filePath string, m map[string]storageEntry) error {
	// Ensure the sstable directory exists
	dirPath := filepath.Dir(filePath)
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		slog.Error("Failed to create sstable directory", "dir", dirPath, "error", err)
		return err
	}

	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		slog.Error("Failed to open SSTable file for writing", "file", filePath, "error", err)
		return err
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	for _, k := range keys {
		v := m[k]
		entry := ssTableEntry{K: k, V: v.Value, Type: v.Type}
		line, err := json.Marshal(entry)
		if err != nil {
			slog.Error("Failed to marshal SSTable entry", "key", k, "error", err)
			continue
		}
		if _, err := f.Write(append(line, '\n')); err != nil {
			slog.Error("Failed to write SSTable entry to file", "key", k, "error", err)
			continue
		}
	}

	if err := f.Sync(); err != nil {
		f.Close()
		slog.Error("Failed to fsync the sstable", "error", err)
		return err
	}

	if err := f.Close(); err != nil {
		slog.Error("Failed to close new sstable file after write", "file", filePath, "error", err)
		return err
	}

	if err := syncParentDir(filePath); err != nil {
		slog.Error("Failed to fsync the sstable dir", "error", err)
		return err
	}

	return nil
}

func (sst *sstableStore) rewriteManifestFromLevels(levels []sstableLevel) error {
	sort.Slice(levels, func(i, j int) bool {
		return levels[i].level < levels[j].level
	})

	mf, err := os.OpenFile(sst.manifestPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		slog.Error("Failed to open manifest file for rewrite", "file", sst.manifestPath, "error", err)
		return err
	}

	for _, levelEntry := range levels {
		if _, err := mf.WriteString(fmt.Sprintf("[L%d]\n", levelEntry.level)); err != nil {
			mf.Close()
			slog.Error("Failed to write manifest header", "file", sst.manifestPath, "error", err)
			return err
		}

		for _, table := range levelEntry.tables {
			if _, err := mf.WriteString(table + "\n"); err != nil {
				mf.Close()
				slog.Error("Failed to write manifest table", "file", sst.manifestPath, "error", err)
				return err
			}
		}

		if _, err := mf.WriteString("\n"); err != nil {
			mf.Close()
			slog.Error("Failed to write manifest separator", "file", sst.manifestPath, "error", err)
			return err
		}
	}

	if err := mf.Sync(); err != nil {
		mf.Close()
		slog.Error("Failed to fsync the manifest file", "error", err)
		return err
	}

	if err := mf.Close(); err != nil {
		slog.Error("Failed to close manifest file after rewrite", "file", sst.manifestPath, "error", err)
		return err
	}

	if err := syncParentDir(sst.manifestPath); err != nil {
		slog.Error("Failed to fsync the manifest dir", "error", err)
		return err
	}

	return nil
}

func (sst *sstableStore) appendTableToManifest(table string, level int) error {
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
	for level := 0; level < MAX_LEVELS; level++ {
		levelTables := sst.getLevelSSTables(level)
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
