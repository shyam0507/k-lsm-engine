package storage

import "path/filepath"

const (
	rootDirName = "data"

	walDirName     = "wal"
	sstableDirName = "sstable"

	walFilePrefix = "wal-"
	ssTablePrefix = "sst-"
	dbFileExt     = ".db"
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
