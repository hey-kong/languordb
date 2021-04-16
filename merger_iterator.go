package languorDB

import (
	"LanguorDB/internalkey"
	"LanguorDB/sstable"
)

type MergingIterator struct {
	list    []*sstable.Iterator
	current *sstable.Iterator
}

func NewMergingIterator(list []*sstable.Iterator) *MergingIterator {
	var iter MergingIterator
	iter.list = list
	return &iter
}

// Returns true if the iterator is positioned at a valid node.
func (it *MergingIterator) Valid() bool {
	return it.current != nil && it.current.Valid()
}

func (it *MergingIterator) InternalKey() *internalkey.InternalKey {
	return it.current.InternalKey()
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *MergingIterator) Next() {
	if it.current != nil {
		it.current.Next()
	}
	it.findSmallest()
}

// Position at the first entry in list.
// Final state of iterator is Valid() if the list is not empty.
func (it *MergingIterator) SeekToFirst() {
	for i := 0; i < len(it.list); i++ {
		it.list[i].SeekToFirst()
	}
	it.findSmallest()
}

func (it *MergingIterator) findSmallest() {
	var smallest *sstable.Iterator = nil
	for i := 0; i < len(it.list); i++ {
		if it.list[i].Valid() {
			if smallest == nil {
				smallest = it.list[i]
			} else if internalkey.InternalKeyComparator(smallest.InternalKey(), it.list[i].InternalKey()) > 0 {
				smallest = it.list[i]
			}
		}
	}
	it.current = smallest
}
