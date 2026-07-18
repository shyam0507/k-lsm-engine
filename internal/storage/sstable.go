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
	var lastLine string
	currentLevel := -1

	var tables []string
	for scanner.Scan() {
		lastLine = scanner.Text()
		levelSep, err := regexp.MatchString(`^\[L[0-9]+\]$`, lastLine)
		if err != nil {
			slog.Error("Error while scanning manifest file", "err", err)
			return levels, err
		}
		if levelSep {
			if currentLevel >= 0 {
				levels = append(levels, sstableLevel{
					tables: tables,
					level:  currentLevel,
				})
			}
			currentLevel++
			//clear the tables array
			tables = nil
		}

		//if level matched break out of loop
		if currentLevel > upToLevel {
			break
		}
		tables = append(tables, lastLine)
	}
	if err := scanner.Err(); err != nil {
		slog.Error("Error while getting data", "err", err)
		return levels, err
	}

	return levels, nil
}

func (sst *sstableStore) getSSTableName(level int) string {
	var count int
	for _, v := range sst.levels {
		if v.level == level {
			count = 1 + len(v.tables)
		}
	}
	return fmt.Sprintf("%s%d%s", ssTablePrefix, count, ssTableExt)
}

func (sst *sstableStore) saveSSTable(m map[string]storageEntry) error {
	slog.Info("SaveSSTable called")
	sstFileName := sst.getSSTableName(0)
	filePath := filepath.Join(sst.dir, sstFileName)

	// Ensure the sstable directory exists
	dirPath := filepath.Dir(filePath)
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		slog.Error("Failed to create sstable directory", "dir", dirPath, "error", err)
		return err
	}

	if err := sst.writeSSTableFile(filePath, m); err != nil {
		slog.Error("Failed to write SSTable file", "file", filePath, "error", err)
		return err
	}

	slog.Info("SSTable file written in JSON Lines format and dir synched", "file", filePath)

	/*
		Now modify the manifest file
	*/

	mf, err := os.OpenFile(sst.manifestPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("Failed to open manifest file", "file", sst.manifestPath, "error", err)
		return err
	}

	if _, err := mf.WriteString(sstFileName + "\n"); err != nil {
		mf.Close()
		slog.Error("Failed to write to manifest file", "file", sst.manifestPath, "error", err)
		return err
	}

	if err := mf.Sync(); err != nil {
		mf.Close()
		slog.Error("Failed to fsync the manifest file", "error", err)
		return err
	}

	if err := mf.Close(); err != nil {
		slog.Error("Failed to close new manifest file after write", "file", sst.manifestPath, "error", err)
		return err
	}

	//fysnc the manifest dir
	if err := syncParentDir(sst.manifestPath); err != nil {
		slog.Error("Failed to fsync the sstable dir", "error", err)
		return err
	}
	slog.Info("SSTable written to disk with name", "name", sstFileName)

	//add the entry to the table
	var level = 0
	for _, v := range sst.levels {
		if v.level == level {
			v.tables = append(v.tables, sstFileName)
		}
	}
	return nil
}

func (sst *sstableStore) writeSSTableFile(filePath string, m map[string]storageEntry) error {
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

func (sst *sstableStore) rewriteManifest(tables []string) error {
	mf, err := os.OpenFile(sst.manifestPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		slog.Error("Failed to open manifest file for rewrite", "file", sst.manifestPath, "error", err)
		return err
	}

	for _, table := range tables {
		if _, err := mf.WriteString(table + "\n"); err != nil {
			mf.Close()
			slog.Error("Failed to write to manifest file", "file", sst.manifestPath, "error", err)
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

func (sst *sstableStore) getKey(key string) (storageEntry, bool) {
	for _, v := range sst.getLevelSSTables(0) {
		f, err := os.Open(filepath.Join(sst.dir, v))
		if err != nil {
			slog.Error("Error while reading the ss table")
			log.Fatal(err)
		}

		scanner := bufio.NewScanner(f)

		for scanner.Scan() {
			line := scanner.Text()
			slog.Info("Read line", "data", line)
			if line == "" {
				continue
			}

			var entry ssTableEntry
			err := json.Unmarshal([]byte(line), &entry)

			if err != nil {
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
			log.Fatalf("error during reading sstable: %s", err)
		}

		f.Close()
	}
	return storageEntry{}, false
}
