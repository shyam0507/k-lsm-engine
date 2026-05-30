package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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

// SSTableEntry represents a key-value pair in the SSTable
type SSTableEntry struct {
	K string `json:"k"`
	V string `json:"v"`
}

// Function to return the ss table name from manifest file
func getSSTableName() (string, error) {
	// Open manifest file in read-only mode to get the last line
	f, err := os.Open(ManifestFilePath)
	if err != nil {
		// If file does not exist, start with sst-1.json
		if os.IsNotExist(err) {
			return "sst-1.json", nil
		}
		return "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var lastLine string
	for scanner.Scan() {
		lastLine = scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		slog.Error("Error while getting data", "err", err)
		return "", err
	}

	var fName string
	if lastLine == "" {
		fName = "sst-1.json"
	} else {
		// Extract the number and increment it
		parts := strings.Split(lastLine, "-")
		if len(parts) < 2 {
			fName = "sst-1.json"
		} else {
			numPart := strings.TrimSuffix(parts[1], ".json")
			// Try to parse and increment
			var num int
			_, err := fmt.Sscanf(numPart, "%d", &num)
			if err != nil {
				num = 1
			} else {
				num++
			}
			fName = fmt.Sprintf("sst-%d.json", num)
		}
	}
	return fName, nil
}

func SaveSSTable(m map[string]string) error {
	slog.Info("SaveSSTable called")
	fileName, err := getSSTableName()
	if err != nil {
		slog.Error("Error while getting SSTable name", "err", err)
		return err
	}

	SSTable := make([]SSTableEntry, 0, len(m))
	for k, v := range m {
		e := SSTableEntry{
			K: k,
			V: v,
		}
		SSTable = append(SSTable, e)
		slog.Debug("Added entry to SSTable", "key", k, "value", v)
	}

	data, err := json.MarshalIndent(SSTable, "", " ")
	if err != nil {
		slog.Error("Failed to marshal SSTable", "error", err)
		return err
	}

	filePath := filepath.Join(DataDir, fileName)
	absFilePath, absErr := filepath.Abs(filePath)
	if absErr != nil {
		slog.Error("Failed to get absolute file path", "file", filePath, "error", absErr)
	} else {
		slog.Info("Absolute SSTable file path", "absFilePath", absFilePath)
	}
	// Ensure the sstable directory exists
	dirPath := filepath.Dir(filePath)
	err = os.MkdirAll(dirPath, 0755)
	if err != nil {
		slog.Error("Failed to create sstable directory", "dir", dirPath, "error", err)
		return err
	}

	err = os.WriteFile(filePath, data, 0644)
	if err != nil {
		slog.Error("Failed to write SSTable file", "file", filePath, "error", err)
		return err
	}
	slog.Info("SSTable file written", "file", filePath)

	// now append the name to manifest file
	f, err := os.OpenFile(ManifestFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("Failed to open manifest file", "file", ManifestFilePath, "error", err)
		return err
	}
	defer f.Close()

	if _, err := f.WriteString(fileName + "\n"); err != nil {
		slog.Error("Failed to write to manifest file", "file", ManifestFilePath, "error", err)
		return err
	}
	slog.Info("SSTable written to disk with name", "name", fileName)

	return nil
}
