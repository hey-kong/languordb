package memtable

import (
	"github.com/hey-kong/languordb/internalkey"
	"github.com/hey-kong/languordb/skiplist"
)

type Iterator struct {
	listIter *skiplist.Iterator
}

// Return true if the position of the iterator is not nil
func (it *Iterator) Valid() bool {
	return it.listIter.Valid()
}

func (it *Iterator) InternalKey() *internalkey.InternalKey {
	return it.listIter.Key().(*internalkey.InternalKey)
}

// Move to the next position
// REQUIRES: Valid()
func (it *Iterator) Next() {
	it.listIter.Next()
}

// Move to the previous position
// REQUIRES: Valid()
func (it *Iterator) Prev() {
	it.listIter.Prev()
}

// Move to the first entry with a internalkey >= target
func (it *Iterator) Seek(target interface{}) {
	it.listIter.Seek(target)
}

// Return the first position in list
func (it *Iterator) SeekToFirst() {
	it.listIter.SeekToFirst()
}

// Return the last position in list
func (it *Iterator) SeekToLast() {
	it.listIter.SeekToLast()
}
