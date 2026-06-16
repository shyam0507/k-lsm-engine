package storage

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log"
	"log/slog"
	"os"
	"path/filepath"
)

type wal struct {
	path string
}

type walEntry struct {
	CRC   string    `json:"crc"`
	Key   string    `json:"key"`
	Type  entryType `json:"type"`
	Value string    `json:"value"`
}

type walPayload struct {
	Key   string    `json:"key"`
	Type  entryType `json:"type"`
	Value string    `json:"value"`
}

func newWAL(dir string) *wal {
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal("Failed to create WAL directory", err)
	}

	return &wal{
		path: filepath.Join(dir, walFileName),
	}
}

func newWALEntry(key, value string, entryType entryType) (*walEntry, error) {
	payload := walPayload{
		Key:   key,
		Type:  entryType,
		Value: value,
	}

	crc, err := calculateCRC(payload)
	if err != nil {
		return nil, err
	}

	return &walEntry{
		CRC:   crc,
		Key:   payload.Key,
		Type:  payload.Type,
		Value: payload.Value,
	}, nil
}

func calculateCRC(payload walPayload) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	sum := crc32.ChecksumIEEE(data)
	return fmt.Sprintf("%08x", sum), nil
}

// append an entry to WAL
func (w *wal) append(e *walEntry) error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		slog.Error("Failed to open WAL file for writing", "file", w.path, "error", err)
		return err
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(e)
	if err != nil {
		slog.Error("Failed to write entry to WAL", "file", w.path, "error", err)
		return err
	}

	return nil
}
