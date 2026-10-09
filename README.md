# k-lsm-engine

A small log-structured merge-tree (LSM) key-value store written in Go, built to learn how storage engines work from the disk up. It has a write-ahead log, a skip-list memtable, block-based SSTables with bloom-filter sidecars, a two-level manifest, and k-way merge compaction. An HTTP server exposes the engine and publishes Prometheus metrics.

Everything is hand-rolled on top of the standard library. The only third-party dependencies are `murmur3` (bloom-filter hashing) and the Prometheus client.

## Quick start

Requires Go 1.24 or newer.

```sh
# start the server on :8080 (data is written to ./data/)
go run ./cmd/server

# in another terminal
curl -X PUT  localhost:8080/hello -d 'world'   # -> world
curl         localhost:8080/hello              # -> world
curl -X DELETE localhost:8080/hello
curl -i      localhost:8080/hello              # -> 404
curl         localhost:8080/metrics            # Prometheus exposition
```

Generate a workload and replay it against the server, checking every `GET` against the expected value:

```sh
go run ./cmd/gen put 10000      # writes put.txt
go run ./cmd/client put.txt     # prints mismatches and total time
```

Run the tests:

```sh
go test ./...
```

Optionally scrape the server with Prometheus using the bundled config:

```sh
prometheus --config.file=prometheus.yml
```

## HTTP API

| Method   | Path        | Behaviour                                                   | Status            |
|----------|-------------|-------------------------------------------------------------|-------------------|
| `GET`    | `/<key>`    | Returns the value as the response body                      | 200, 404 if absent |
| `PUT`    | `/<key>`    | Stores the request body as the value and echoes it back     | 200               |
| `DELETE` | `/<key>`    | Writes a tombstone for the key                              | 200 (always)      |
| other    | `/<key>`    | Rejected                                                    | 405 with `Allow`  |
| `GET`    | `/metrics`  | Prometheus metrics                                          | 200               |

Request latency is recorded in the histogram `klsm_http_request_duration_seconds{method,path}`. The `path` label is always `/key`.

## Repository layout

```
cmd/
  server/   HTTP front end on :8080
  client/   replays a workload file against the server and verifies GET results
  gen/      generates workload files (put | put-delete | put-delete-scan)
internal/storage/
  engine.go              Engine: Put / Get / Delete, recovery, flush and compaction triggers
  memtable.go            RWMutex wrapper around the skip list
  memtable_skiplist.go   skip list (16 levels, p = 0.5), one node per key
  wal.go                 JSON-lines write-ahead log with CRC32 per record
  sstable.go             sstableStore: manifest, levels, bloom loading, L0 flush, read path
  block_based_table.go   on-disk SSTable format, reader and writer
  bloom_filter.go        bloom filter and .bloom sidecar format
  compaction.go          k-way heap merge of L0 + L1 into L1
  entry.go               entry types (PUT / DELETE) and the in-memory entry struct
  constants.go           data directory layout
  fsync.go               parent-directory fsync helper
prometheus.yml           scrape config for localhost:8080
```

The engine lives under `internal/`, so it cannot be imported from outside this module. Its API is:

```go
func NewEngine() *Engine
func (e *Engine) Put(key, value string)
func (e *Engine) Get(key string) (string, bool)
func (e *Engine) Delete(key string)
```

## Architecture

### Write path

1. Take the engine-wide write lock.
2. Append the record to the WAL. The file is opened, written, fsynced and closed, and the parent directory is fsynced.
3. Insert into the skip-list memtable. An existing key is overwritten in place, so the memtable holds one node per key.
4. When the memtable reaches `FLUSH_THRESHOLD` distinct keys, flush it synchronously before releasing the lock.

A `Delete` is a `Put` of a tombstone and counts toward the flush threshold.

### Flush

1. Take a sorted snapshot of the memtable.
2. Write it to a temporary SSTable file, fsync, then write the bloom-filter sidecar, rename both into place and fsync the directory.
3. Rewrite the manifest atomically (temp file, rename, directory fsync) with the new table added to L0. If this fails the new table and its sidecar are removed.
4. Register the bloom filter in memory, truncate the WAL and clear the memtable.
5. Compact if L0 is now full.

### Read path

1. Check the memtable. A tombstone means not found.
2. Check L0 tables, newest first.
3. Check L1 tables, skipping any whose `[min, max]` key range does not contain the key.

Every table is consulted through its bloom filter first. Within a table the reader binary-searches the index page for the data block, verifies the block's CRC, then binary-searches inside the block. The first hit wins; a tombstone anywhere means not found.

### Compaction

When L0 holds `COMPACTION_THRESHOLD` tables, all L0 and all L1 tables are merged with a min-heap ordered by key, then by level (L0 before L1), then by table age (newer first). The newest version of each key is kept. Output is split into new L1 tables of at most 1953 entries, each with its min/max key recorded in the manifest. The manifest is rewritten with L0 empty, then the old tables and sidecars are deleted. Compaction runs synchronously inside the flush, under the write lock.

### Recovery

On start-up the engine replays the WAL into the memtable, stopping at the first record whose CRC or JSON is invalid. It then parses the manifest, drops any table listed there that no longer exists on disk (and deletes its orphaned sidecar), and loads the bloom sidecars. A missing or corrupt sidecar is logged and that table is read without a bloom check.

## On-disk formats

All paths are relative to the process's working directory:

```
data/
  wal/wal.db
  sstable/
    manifest.db
    l0/sst-N.db   sst-N.db.bloom
    l1/sst-N.db   sst-N.db.bloom
```

### WAL

One JSON object per line. The CRC is CRC32-IEEE over the JSON encoding of `{key, type, value}`.

```json
{"crc":"bfb92d7a","key":"hello","type":"PUT","value":"world"}
```

The file is truncated to zero bytes after every successful flush.

### SSTable

A table is a sequence of 4 KiB pages: one index page followed by up to 63 data pages. There is no footer.

Index page:

| Offset        | Contents                                                        |
|---------------|-----------------------------------------------------------------|
| `0`           | number of data blocks (1 byte)                                  |
| `64 + i*64`   | first key of data block `i`, NUL-padded to 64 bytes             |

Data page:

| Offset        | Contents                                                        |
|---------------|-----------------------------------------------------------------|
| `0..8`        | CRC64-ECMA (little-endian) of bytes `8..4096`                   |
| `8`           | entry count (1 byte, at most 31)                                |
| `128 + i*128` | entry `i`: 64-byte key followed by 64-byte value, NUL-padded    |

A table therefore holds at most 63 × 31 = 1953 entries. Keys and values are limited to 64 bytes, must be non-empty, and must not contain NUL bytes. Entries are strictly sorted by key. A tombstone is stored as an all-zero value field; the reader turns any all-zero value back into a DELETE.

### Bloom sidecar

Each table has a `<table>.bloom` file holding a 2096-bit filter with 5 hash functions, computed by double hashing two murmur3 values. The sidecar is a 32-byte header followed by the raw bits:

| Offset   | Contents                                  |
|----------|-------------------------------------------|
| `0..8`   | magic `KLSMBF01`                          |
| `8`      | version (`1`)                             |
| `12..16` | filter length in bits (u32 LE)            |
| `16..20` | hash count (u32 LE)                       |
| `20..24` | bit-array length in bytes (u32 LE)        |
| `24..32` | CRC64-ECMA of the bit array               |

### Manifest

A plain-text file with one section per level and one table per line. L1 lines also carry the table's min and max key, Go-quoted and tab-separated.

```
[L0]
sst-1.db
sst-2.db

[L1]
sst-1.db	"aaaaa"	"mzzzz"
sst-2.db	"nabcd"	"zzzzz"
```

The manifest is always rewritten in full and swapped in atomically.

## Tuning constants

Every knob is a compile-time constant. There are no flags, environment variables or options struct.

| Constant                   | Value | Meaning                                   | File                    |
|----------------------------|-------|-------------------------------------------|-------------------------|
| `FLUSH_THRESHOLD`          | 1953  | distinct keys in the memtable before flush | `engine.go`             |
| `COMPACTION_THRESHOLD`     | 5     | L0 tables before compaction               | `engine.go`             |
| `MAX_LEVELS`               | 2     | number of levels (L0, L1)                 | `engine.go`             |
| `sstableBlockSize`         | 4096  | page size in bytes                        | `block_based_table.go`  |
| `sstableIndexCapacity`     | 63    | data blocks per table                     | `block_based_table.go`  |
| `sstableDataBlockEntries`  | 31    | entries per data block                    | `block_based_table.go`  |
| `sstableKeyValueSize`      | 64    | max key and max value size in bytes       | `block_based_table.go`  |
| `bloomFilterLength`        | 2096  | bits per bloom filter                     | `bloom_filter.go`       |
| `bloomFilterHashes`        | 5     | hash functions per bloom filter           | `bloom_filter.go`       |
| `skipListMaxLevels`        | 16    | skip-list height                          | `memtable_skiplist.go`  |

The server port (`:8080`) and the data directory (`./data`) are hardcoded as well.

## Work in progress

Planned next steps, roughly in order:

- [ ] Validate keys and values at `Put` time (64-byte limit, non-empty, no NUL bytes) instead of failing at the next flush.
- [ ] Return errors from `Put` and `Delete` so the HTTP server can report failed writes.
- [ ] Move flush and compaction off the write lock into background work.
- [ ] Drop tombstones during compaction once they reach the bottom level.
- [ ] Add range scans, a `Close` method and a block cache.

## Development

```sh
go build ./...
go vet ./...
go test ./internal/storage/...
```

All engine tests run against a temporary data directory created with `t.TempDir()` through the `newTestEngine` helper in `engine_test.go`. The `data/` directory is ignored by git; the workload files produced by `cmd/gen` are not, so delete them or move them out of the repo after use.
