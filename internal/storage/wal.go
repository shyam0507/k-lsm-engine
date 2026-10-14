package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

type wal struct {
	path string
	f    *os.File
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

func newWAL(dir string, num int) *wal {
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal("Failed to create WAL directory", err)
	}

	path := filepath.Join(dir, fmt.Sprintf("%s%d%s", walFilePrefix, num, dbFileExt))

	// Keep one fd open for the life of the WAL: appends then cost a single fsync
	// instead of open + fsync + close + directory fsync.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal("Failed to open WAL file", err)
	}

	// The directory entry only needs to be durable once, when the file is created.
	if err := syncParentDir(path); err != nil {
		log.Fatal("Failed to fsync WAL directory", err)
	}

	return &wal{
		path: path,
		f:    f,
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
	if err := json.NewEncoder(w.f).Encode(e); err != nil {
		slog.Error("Failed to write entry to WAL", "file", w.path, "error", err)
		return err
	}

	syncStart := time.Now()
	if err := w.f.Sync(); err != nil {
		slog.Error("Failed to fsync the wal", "error", err)
		return err
	}
	observeStage("write", "wal_fsync", syncStart)

	return nil
}

func (w *wal) truncate() error {
	// The fd is O_APPEND, so the next write lands at the new end (offset 0).
	if err := w.f.Truncate(0); err != nil {
		slog.Error("Failed to truncate the wal", "file", w.path, "error", err)
		return err
	}

	if err := w.f.Sync(); err != nil {
		slog.Error("Failed to fsync the wal on truncate", "error", err)
		return err
	}

	// truncation does not change directory entries, so no dir sync required.

	return nil
}

func (w *wal) getAll() ([]walPayload, error) {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		slog.Error("Failed to open WAL file for reading", "file", w.path, "error", err)
		return nil, err
	}
	defer f.Close()

	entries := make([]walPayload, 0, 100)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		b := scanner.Bytes()

		var walEnt walEntry
		err := json.Unmarshal(b, &walEnt)
		if err != nil {
			slog.Error("Error while unmarshalling wal entry", "err", err)
			return entries, err
		}

		//Check the crc
		walPay := walPayload{
			Key:   walEnt.Key,
			Value: walEnt.Value,
			Type:  walEnt.Type,
		}

		crc, err := calculateCRC(walPay)
		if err != nil {
			slog.Error("Error while calculating the CRC", "err", err)
			return entries, err
		}

		if crc != walEnt.CRC {
			err := fmt.Errorf("calculated crc %s and stored crc %s do not match", crc, walEnt.CRC)
			slog.Error("calculated crc and stored crc does not match", "err", err)
			return entries, err
		}

		entries = append(entries, walPay)

	}

	if err := scanner.Err(); err != nil {
		slog.Error("Error while getting data from wal", "err", err)
		return entries, err
	}

	return entries, nil

}
