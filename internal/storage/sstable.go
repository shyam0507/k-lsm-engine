package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Always use project root for data directory
var (
	DataDir          string
	ManifestFilePath string
)

func init() {
	DataDir = filepath.Join("data", "sstable")
	ManifestFilePath = filepath.Join(DataDir, "manifest.txt")
}

// ssTableEntry represents a key-value pair in the table
type ssTableEntry struct {
	K string `json:"k"`
	V string `json:"v"`
}

type ssTable struct {
	tables  []string
	counter int //for the sstable name
}

func NewSSTable() *ssTable {
	tables, count, err := getAllSSTables()

	if err != nil {
		log.Fatal("Error while parsing/loading sstable", err)
	}

	return &ssTable{
		tables:  tables,
		counter: count,
	}
}

// Function to return the ss table name from manifest file
func getAllSSTables() ([]string, int, error) {
	tables := make([]string, 0)
	counter := 0

	f, err := os.Open(ManifestFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return tables, counter, nil
		}
		return tables, counter, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var lastLine string
	for scanner.Scan() {
		lastLine = scanner.Text()
		tables = append(tables, lastLine)
	}
	if err := scanner.Err(); err != nil {
		slog.Error("Error while getting data", "err", err)
		return tables, counter, err
	}

	if lastLine != "" {
		parts := strings.Split(lastLine, "-")
		if len(parts) >= 2 {
			numPart := strings.TrimSuffix(parts[1], ".txt")
			_, err := fmt.Sscanf(numPart, "%d", &counter)
			if err != nil {
				counter = 0
			}
		}
	}

	//Reverse the table because the latest information will be in latest table
	slices.Reverse(tables)
	return tables, counter, nil
}

func (sst *ssTable) getSSTableName() string {
	sst.counter++
	return fmt.Sprintf("sst-%d.txt", sst.counter)
}

func (sst *ssTable) saveSSTable(m map[string]string) error {
	slog.Info("SaveSSTable called")
	fileName := sst.getSSTableName()

	filePath := filepath.Join(DataDir, fileName)
	absFilePath, absErr := filepath.Abs(filePath)
	if absErr != nil {
		slog.Error("Failed to get absolute file path", "file", filePath, "error", absErr)
	} else {
		slog.Info("Absolute table file path", "absFilePath", absFilePath)
	}
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
	defer f.Close()

	//sort keys before storing
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	for _, k := range keys {
		v := m[k]
		entry := ssTableEntry{K: k, V: m[k]}
		line, err := json.Marshal(entry)
		if err != nil {
			slog.Error("Failed to marshal SSTable entry", "key", k, "error", err)
			return err
		}
		if _, err := f.Write(append(line, '\n')); err != nil {
			slog.Error("Failed to write SSTable entry to file", "key", k, "error", err)
			return err
		}
		slog.Debug("Added entry to SSTable", "key", k, "value", v)
	}
	slog.Info("SSTable file written in JSON Lines format", "file", filePath)

	// now append the name to manifest file
	mf, err := os.OpenFile(ManifestFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("Failed to open manifest file", "file", ManifestFilePath, "error", err)
		return err
	}
	defer mf.Close()

	if _, err := mf.WriteString(fileName + "\n"); err != nil {
		slog.Error("Failed to write to manifest file", "file", ManifestFilePath, "error", err)
		return err
	}
	slog.Info("SSTable written to disk with name", "name", fileName)

	//add the entry to the table
	sst.tables = append([]string{fileName}, sst.tables...)

	return nil
}

func (sst *ssTable) getKey(key string) (string, bool) {
	for _, v := range sst.tables {
		f, err := os.Open(filepath.Join(DataDir, v))
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
				return entry.V, true
			}
		}

		if err := scanner.Err(); err != nil {
			log.Fatalf("error during reading sstable: %s", err)
		}
	}
	return "", false
}
