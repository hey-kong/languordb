package sstable

import (
	"LanguorDB/internalkey"
	"LanguorDB/sstable/block"
)

type Iterator struct {
	table           *SSTable
	dataBlockHandle BlockHandle
	dataIter        *block.Iterator
	indexIter       *block.Iterator
}

// Returns true if the iterator is positioned at a valid node.
func (it *Iterator) Valid() bool {
	return it.dataIter != nil && it.dataIter.Valid()
}

func (it *Iterator) InternalKey() *internalkey.InternalKey {
	return it.dataIter.InternalKey()
}

func (it *Iterator) Key() []byte {
	return it.InternalKey().UserKey
}

func (it *Iterator) Value() []byte {
	return it.InternalKey().UserValue
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *Iterator) Next() {
	it.dataIter.Next()
	it.skipEmptyDataBlocksForward()
}

// Advances to the previous position.
// REQUIRES: Valid()
func (it *Iterator) Prev() {
	it.dataIter.Prev()
	it.skipEmptyDataBlocksBackward()
}

// Advance to the first entry with a internalkey >= target
func (it *Iterator) Seek(target []byte) {
	// Index Block的block_data字段中，每一条记录的key都满足：
	// 大于等于Data Block的所有key，并且小于后面所有Data Block的key
	it.indexIter.Seek(target)
	it.initDataBlock()
	if it.dataIter != nil {
		it.dataIter.Seek(target)
	}
	it.skipEmptyDataBlocksForward()
}

// Position at the first entry in list.
// Final state of iterator is Valid() if the list is not empty.
func (it *Iterator) SeekToFirst() {
	it.indexIter.SeekToFirst()
	it.initDataBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToFirst()
	}
	it.skipEmptyDataBlocksForward()
}

// Position at the last entry in list.
// Final state of iterator is Valid() if the list is not empty.
func (it *Iterator) SeekToLast() {
	it.indexIter.SeekToLast()
	it.initDataBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToLast()
	}
	it.skipEmptyDataBlocksBackward()
}

func (it *Iterator) initDataBlock() {
	if !it.indexIter.Valid() {
		it.dataIter = nil
	} else {
		var index IndexBlockHandle
		index.InternalKey = it.indexIter.InternalKey()
		tmpBlockHandle := index.GetBlockHandle()

		if it.dataIter != nil && it.dataBlockHandle == tmpBlockHandle {
			// data_iter_ is already constructed with this iterator, so
			// no need to change anything
		} else {
			it.dataIter = it.table.readBlock(tmpBlockHandle).NewIterator()
			it.dataBlockHandle = tmpBlockHandle
		}
	}
}

func (it *Iterator) skipEmptyDataBlocksForward() {
	for it.dataIter == nil || !it.dataIter.Valid() {
		if !it.indexIter.Valid() {
			it.dataIter = nil
			return
		}
		it.indexIter.Next()
		it.initDataBlock()
		if it.dataIter != nil {
			it.dataIter.SeekToFirst()
		}
	}
}

func (it *Iterator) skipEmptyDataBlocksBackward() {
	for it.dataIter == nil || !it.dataIter.Valid() {
		if !it.indexIter.Valid() {
			it.dataIter = nil
			return
		}
		it.indexIter.Prev()
		it.initDataBlock()
		if it.dataIter != nil {
			it.dataIter.SeekToLast()
		}
	}
}
