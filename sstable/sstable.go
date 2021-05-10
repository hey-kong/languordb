package sstable

import (
	"io"
	"os"

	"github.com/golang/snappy"
	"github.com/hey-kong/languordb/errors"
	"github.com/hey-kong/languordb/internalkey"
	"github.com/hey-kong/languordb/sstable/block"
)

type SSTable struct {
	index  *block.Block
	footer Footer
	file   *os.File
}

func Open(fileName string) (*SSTable, error) {
	var table SSTable
	var err error
	table.file, err = os.Open(fileName)
	if err != nil {
		return nil, err
	}
	stat, _ := table.file.Stat()
	// Read the footer block
	footerSize := int64(table.footer.Size())
	if stat.Size() < footerSize {
		return nil, errors.ErrTableFileTooShort
	}

	_, err = table.file.Seek(-footerSize, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	err = table.footer.DecodeFrom(table.file)
	if err != nil {
		return nil, err
	}
	// Read the index block
	table.index = table.readBlock(table.footer.IndexHandle)
	return &table, nil
}

func (table *SSTable) NewIterator() *Iterator {
	var it Iterator
	it.table = table
	it.indexIter = table.index.NewIterator()
	return &it
}

func (table *SSTable) Get(key []byte) ([]byte, error) {
	it := table.NewIterator()
	it.Seek(key)
	if it.Valid() {
		internalKey := it.InternalKey()
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

func (table *SSTable) readBlock(blockHandle BlockHandle) *block.Block {
	p := make([]byte, blockHandle.Size)
	n, err := table.file.ReadAt(p, int64(blockHandle.Offset))
	if err != nil || uint32(n) != blockHandle.Size {
		return nil
	}
	p, _ = snappy.Decode(nil, p)
	return block.New(p)
}
