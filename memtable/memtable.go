package memtable

import (
	"github.com/hey-kong/languordb/errors"
	"github.com/hey-kong/languordb/internalkey"
	"github.com/hey-kong/languordb/skiplist"
)

type MemTable struct {
	table       *skiplist.SkipList
	memoryUsage uint64
}

func New() *MemTable {
	var memTable MemTable
	memTable.table = skiplist.NewSkipList(internalkey.InternalKeyComparator)
	return &memTable
}

func (memTable *MemTable) NewIterator() *Iterator {
	return &Iterator{listIter: skiplist.NewIterator(memTable.table)}
}

func (memTable *MemTable) Add(seq uint64, valueType internalkey.ValueType, key, value []byte) {
	internalKey := internalkey.NewInternalKey(seq, valueType, key, value)

	memTable.memoryUsage += uint64(16 + len(key) + len(value))
	memTable.table.Insert(internalKey, nil)
}

func (memTable *MemTable) Get(key []byte) ([]byte, error) {
	lookupKey := internalkey.LookupKey(key)

	it := skiplist.NewIterator(memTable.table)
	it.Seek(lookupKey)
	if it.Valid() {
		internalKey := it.Key().(*internalkey.InternalKey)
		if internalkey.UserKeyComparator(key, internalKey.UserKey) == 0 {
			// 判断valueType
			if internalKey.Type == internalkey.TypeValue {
				return internalKey.UserValue, nil
			} else {
				return nil, errors.ErrDeletion
			}
		}
	}
	return nil, errors.ErrNotFound
}

func (memTable *MemTable) ApproximateMemoryUsage() uint64 {
	return memTable.memoryUsage
}
