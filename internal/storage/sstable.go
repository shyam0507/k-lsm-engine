package storage

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
)

const (
	MANIFEST_FILE_NAME = "manifest.txt"
	SSTABLE_FILE_PATH  = "./data/sstable"
)

type SSTableEntry struct {
	k string
	v string
}

func SaveSSTable(m map[string]string, fileName string) error {
	slog.Info("SaveSSTable called", "fileName", fileName)
	SSTable := make([]SSTableEntry, 0, len(m))
	for k, v := range m {
		e := SSTableEntry{
			k: k,
			v: v,
		}
		SSTable = append(SSTable, e)
		slog.Debug("Added entry to SSTable", "key", k, "value", v)
	}

	data, err := json.MarshalIndent(SSTable, "", " ")
	if err != nil {
		slog.Error("Failed to marshal SSTable", "error", err)
		return err
	}

	filePath := fmt.Sprintf("%s/%s", SSTABLE_FILE_PATH, fileName)
	err = os.WriteFile(filePath, data, 0644)
	if err != nil {
		slog.Error("Failed to write SSTable file", "file", filePath, "error", err)
		return err
	}
	slog.Info("SSTable file written", "file", filePath)

	// now append the name to manifest file
	manifestPath := fmt.Sprintf("%s/%s", SSTABLE_FILE_PATH, MANIFEST_FILE_NAME)
	f, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("Failed to open manifest file", "file", manifestPath, "error", err)
		return err
	}
	defer f.Close()

	f.WriteString(fileName + "\n")
	slog.Info("SSTable written to disk with name", "name", fileName)

	return nil
}
