package storage

import (
	"log"
	"os"
	"sync"
	"sync/atomic"
)

type Catalog struct {
	memtables    []*memTable // [0]=active, rest=immutable
	walPaths     []string    // same length/order as memtables, or active+frozen paths
	manifestPath string
	version      int64
	refCount     atomic.Int64
}

type CatalogManager struct {
	mu          sync.Mutex
	catalogs    []*Catalog
	active      *Catalog
	nextVersion int64
}

func newCatalogManager() *CatalogManager {
	return &CatalogManager{}
}

// TBD how to send 2 memtables at once?
func (cm *CatalogManager) NewCatalog(memtables []*memTable, walPaths []string, manifestPath string) *Catalog {
	if len(memtables) == 0 || len(memtables) != len(walPaths) {
		log.Fatalf("CatalogManager: memtables and walPaths must be non-empty and the same length")
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.nextVersion++
	catalog := &Catalog{
		memtables:    append([]*memTable(nil), memtables...),
		walPaths:     append([]string(nil), walPaths...),
		manifestPath: manifestPath,
		version:      cm.nextVersion,
	}
	cm.catalogs = append(cm.catalogs, catalog)
	cm.active = catalog
	return catalog
}

func (cm *CatalogManager) AcquireCatalog() *Catalog {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.active == nil {
		log.Fatalf("CatalogManager: no active catalog")
		return nil
	}

	cm.active.refCount.Add(1)
	return cm.active
}

func (cm *CatalogManager) ReleaseCatalog(catalog *Catalog) {
	if catalog == nil {
		return
	}
	if catalog.refCount.Add(-1) != 0 {
		return
	}
	cm.mu.Lock()
	if catalog.refCount.Load() != 0 || catalog == cm.active {
		cm.mu.Unlock()
		return
	}
	index := -1
	for i, c := range cm.catalogs {
		if c == catalog {
			index = i
			break
		}
	}
	if index == -1 {
		cm.mu.Unlock()
		return
	}
	cm.catalogs = append(cm.catalogs[:index], cm.catalogs[index+1:]...)
	stale := stalePaths(catalog, cm.catalogs)
	cm.mu.Unlock()
	for _, path := range stale {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			log.Printf("CatalogManager: failed to remove %s: %v", path, err)
		}
	}
}

func stalePaths(old *Catalog, remaining []*Catalog) []string {
	live := make(map[string]struct{})
	for _, c := range remaining {
		for _, p := range c.walPaths {
			if p != "" {
				live[p] = struct{}{}
			}
		}
		if c.manifestPath != "" {
			live[c.manifestPath] = struct{}{}
		}
	}
	var stale []string
	add := func(p string) {
		if p == "" {
			return
		}
		if _, ok := live[p]; !ok {
			stale = append(stale, p)
		}
	}
	for _, p := range old.walPaths {
		add(p)
	}
	add(old.manifestPath)
	return stale
}
