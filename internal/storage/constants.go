package storage

import "path/filepath"

const (
	rootDirName = "data"

	walDirName     = "wal"
	sstableDirName = "sstable"

	walFileName   = "wal.db"
	ssTablePrefix = "sst-"
)

func rootDirPath() string {
	return rootDirName
}

func walDirPath() string {
	return filepath.Join(rootDirPath(), walDirName)
}

func sstableDirPath() string {
	return filepath.Join(rootDirPath(), sstableDirName)
}
