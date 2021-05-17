package languordb

import (
	"github.com/hey-kong/languordb/internalkey"
	"github.com/hey-kong/languordb/sstable"
)

type MergedIterator struct {
	list    []*sstable.Iterator
	current *sstable.Iterator
}

func NewMergedIterator(list []*sstable.Iterator) *MergedIterator {
	var iter MergedIterator
	iter.list = list
	return &iter
}

// Return true if the iterator is on a valid node
func (it *MergedIterator) Valid() bool {
	return it.current != nil && it.current.Valid()
}

func (it *MergedIterator) InternalKey() *internalkey.InternalKey {
	return it.current.InternalKey()
}

// Advances to the next position
// REQUIRES: Valid()
func (it *MergedIterator) Next() {
	if it.current != nil {
		it.current.Next()
	}
	it.findSmallest()
}

// Position at the first entry in list
// Final state of iterator is Valid() if the list is not empty
func (it *MergedIterator) SeekToFirst() {
	for i := 0; i < len(it.list); i++ {
		it.list[i].SeekToFirst()
	}
	it.findSmallest()
}

func (it *MergedIterator) findSmallest() {
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
