package block

import (
	"github.com/hey-kong/languordb/internalkey"
)

type Iterator struct {
	block *Block
	index int
}

// Return true if the iterator is on a valid node
func (it *Iterator) Valid() bool {
	return it.index >= 0 && it.index < len(it.block.items)
}

func (it *Iterator) InternalKey() *internalkey.InternalKey {
	return &it.block.items[it.index]
}

// Advance to the next position
// REQUIRES: Valid()
func (it *Iterator) Next() {
	it.index++
}

// Advance to the previous position
// REQUIRES: Valid()
func (it *Iterator) Prev() {
	it.index--
}

// Advance to the first entry with a internalkey >= target
func (it *Iterator) Seek(target interface{}) {
	// 二分法查询
	left := 0
	right := len(it.block.items) - 1
	for left < right {
		mid := (left + right) / 2
		if internalkey.UserKeyComparator(it.block.items[mid].UserKey, target) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	if left == len(it.block.items)-1 {
		if internalkey.UserKeyComparator(it.block.items[left].UserKey, target) < 0 {
			// not found
			left++
		}
	}
	it.index = left
}

// Position at the first entry in list
// Final state of iterator is Valid() if the list is not empty
func (it *Iterator) SeekToFirst() {
	it.index = 0
}

// Position at the last entry in list
// Final state of iterator is Valid() if the list is not empty
func (it *Iterator) SeekToLast() {
	if len(it.block.items) > 0 {
		it.index = len(it.block.items) - 1
	}
}
