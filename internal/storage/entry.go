package storage

type entryType string

const (
	entryTypePut    entryType = "PUT"
	entryTypeDelete entryType = "DELETE"
)

type storageEntry struct {
	Type  entryType
	Value string
}
