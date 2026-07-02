package storage

import (
	"os"
	"path/filepath"
)

// syncParentDir ensures the directory entry for the given path is flushed
// to persistent storage. This is a best-effort helper used after creating
// or appending to files where the directory entry must be durable.
func syncParentDir(path string) error {
	dir := filepath.Dir(path)
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
