package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"os"
	"path/filepath"

	"github.com/spaolacci/murmur3"
)

const (
	bloomFilterLength      = 2096
	bloomFilterHashes      = 5
	bloomSidecarSuffix     = ".bloom"
	bloomSidecarHeaderSize = 32
)

var (
	bloomSidecarMagic = [8]byte{'K', 'L', 'S', 'M', 'B', 'F', '0', '1'}
	bloomCRCTable     = crc64.MakeTable(crc64.ECMA)
)

type bloomFilter struct {
	bits   [bloomFilterLength / 8]byte
	length int
	hashes int
}

func newBloomFilter() *bloomFilter {
	return &bloomFilter{length: bloomFilterLength, hashes: bloomFilterHashes}
}

func bloomFilterForEntries(entries []ssTableEntry) bloomFilter {
	filter := newBloomFilter()
	for _, entry := range entries {
		filter.add(entry.K)
	}
	return *filter
}

func bloomSidecarPath(sstablePath string) string {
	return sstablePath + bloomSidecarSuffix
}

func hash(s string) (uint32, uint32) {
	h1 := murmur3.Sum32([]byte(s))
	h2 := murmur3.Sum32([]byte(s + "|salt"))
	return h1, h2
}

func (bf *bloomFilter) add(s string) {
	h1, h2 := hash(s)

	for i := 0; i < bf.hashes; i++ {
		idx := (h1 + uint32(i)*h2) % uint32(bf.length)
		bf.bits[idx/8] |= 1 << (idx % 8)
	}
}

func (bf *bloomFilter) found(s string) bool {
	h1, h2 := hash(s)
	for i := 0; i < bf.hashes; i++ {
		idx := (h1 + uint32(i)*h2) % uint32(bf.length)
		if bf.bits[idx/8]&(1<<(idx%8)) == 0 {
			return false
		}
	}
	return true
}

func writeBloomFilterSidecar(sstablePath string, filter bloomFilter) error {
	if filter.length != bloomFilterLength || filter.hashes != bloomFilterHashes {
		return fmt.Errorf("unsupported bloom filter parameters: length=%d hashes=%d", filter.length, filter.hashes)
	}

	sidecarPath := bloomSidecarPath(sstablePath)
	f, err := os.CreateTemp(filepath.Dir(sidecarPath), "."+filepath.Base(sidecarPath)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := f.Name()
	committed := false
	defer func() {
		if !committed {
			_ = f.Close()
			_ = os.Remove(tempPath)
		}
	}()
	if err := f.Chmod(0644); err != nil {
		return err
	}

	header := make([]byte, bloomSidecarHeaderSize)
	copy(header[:8], bloomSidecarMagic[:])
	header[8] = 1
	binary.LittleEndian.PutUint32(header[12:16], uint32(filter.length))
	binary.LittleEndian.PutUint32(header[16:20], uint32(filter.hashes))
	binary.LittleEndian.PutUint32(header[20:24], uint32(len(filter.bits)))
	binary.LittleEndian.PutUint64(header[24:32], crc64.Checksum(filter.bits[:], bloomCRCTable))
	if _, err := f.Write(header); err != nil {
		return err
	}
	if _, err := f.Write(filter.bits[:]); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, sidecarPath); err != nil {
		return err
	}
	committed = true
	return syncParentDir(sidecarPath)
}

func readBloomFilterSidecar(sstablePath string) (bloomFilter, error) {
	contents, err := os.ReadFile(bloomSidecarPath(sstablePath))
	if err != nil {
		return bloomFilter{}, err
	}
	if len(contents) != bloomSidecarHeaderSize+bloomFilterLength/8 {
		return bloomFilter{}, fmt.Errorf("invalid bloom sidecar size %d", len(contents))
	}
	header := contents[:bloomSidecarHeaderSize]
	if string(header[:8]) != string(bloomSidecarMagic[:]) || header[8] != 1 {
		return bloomFilter{}, fmt.Errorf("invalid bloom sidecar header")
	}
	length := binary.LittleEndian.Uint32(header[12:16])
	hashes := binary.LittleEndian.Uint32(header[16:20])
	bitBytes := binary.LittleEndian.Uint32(header[20:24])
	if length != bloomFilterLength || hashes != bloomFilterHashes || bitBytes != bloomFilterLength/8 {
		return bloomFilter{}, fmt.Errorf("unsupported bloom sidecar parameters")
	}
	bits := contents[bloomSidecarHeaderSize:]
	if crc64.Checksum(bits, bloomCRCTable) != binary.LittleEndian.Uint64(header[24:32]) {
		return bloomFilter{}, fmt.Errorf("bloom sidecar checksum mismatch")
	}

	filter := bloomFilter{length: int(length), hashes: int(hashes)}
	copy(filter.bits[:], bits)
	return filter, nil
}
