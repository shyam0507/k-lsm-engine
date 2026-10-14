package storage

import (
	"math/rand/v2"
)

const skipListMaxLevels = 16

type node struct {
	key   string
	value storageEntry
	next  []*node
}

type skipList struct {
	// head is a sentinel: it is not a user-visible entry and always has every
	// possible level. This lets every insertion use the same linking logic.
	head   *node
	levels int // highest currently populated level, in the range [1, skipListMaxLevels]
	size   int
}

func newSkipList() *skipList {
	return &skipList{
		head:   &node{next: make([]*node, skipListMaxLevels)},
		levels: 1,
		size:   0,
	}
}

// function to decide upto which level key should be set
func generateLevel() int {
	level := 1

	for level < skipListMaxLevels && rand.N(2) == 0 {
		level++
	}
	return level
}

func (l *skipList) add(key string, val storageEntry) {
	// update[level] is the node immediately before key at that level.
	update := make([]*node, skipListMaxLevels)
	current := l.head
	for i := l.levels - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		update[i] = current
	}

	// Keep one entry per key; adding an existing key replaces its value.
	if next := update[0].next[0]; next != nil && next.key == key {
		next.value = val
		return
	}

	level := generateLevel()
	if level > l.levels {
		for i := l.levels; i < level; i++ {
			update[i] = l.head
		}
		l.levels = level
	}

	node := &node{key: key, value: val, next: make([]*node, level)}
	for i := 0; i < level; i++ {
		node.next[i] = update[i].next[i]
		update[i].next[i] = node
	}

	l.size++
}

func (l *skipList) get(key string) (storageEntry, bool) {
	current := l.head
	for i := l.levels - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
	}

	current = current.next[0]
	if current != nil && current.key == key {
		return current.value, true
	}

	return storageEntry{}, false
}

// entries returns a point-in-time, key-sorted snapshot of the list.
func (l *skipList) entries() []ssTableEntry {
	entries := make([]ssTableEntry, 0, l.size)
	for current := l.head.next[0]; current != nil; current = current.next[0] {
		entries = append(entries, ssTableEntry{
			K:    current.key,
			V:    current.value.Value,
			Type: current.value.Type,
		})
	}

	return entries
}

func (l *skipList) clear() bool {
	l.head.next = make([]*node, skipListMaxLevels)
	l.levels = 1
	l.size = 0

	return true
}
