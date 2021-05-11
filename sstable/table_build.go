package sstable

import (
	"log"
	"os"
	"syscall"

	"github.com/golang/snappy"
	"github.com/hey-kong/languordb/internalkey"
	"github.com/hey-kong/languordb/sstable/block"
)

const (
	MaxBlockSize = 4 * 1024
)

type TableBuilder struct {
	file               *os.File
	offset             uint32
	numEntries         int32
	dataBlockBuilder   block.Builder
	indexBlockBuilder  block.Builder
	pendingIndexEntry  bool
	pendingIndexHandle IndexBlockHandle
	status             error
}

func NewTableBuilder(fileName string) *TableBuilder {
	var builder TableBuilder
	var err error
	builder.file, err = os.Create(fileName)
	if err != nil {
		return nil
	}
	builder.pendingIndexEntry = false
	return &builder
}

func (builder *TableBuilder) FileSize() uint32 {
	return builder.offset
}

func (builder *TableBuilder) Add(internalKey *internalkey.InternalKey) {
	if builder.status != nil {
		return
	}
	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.Add(builder.pendingIndexHandle.InternalKey)
		builder.pendingIndexEntry = false
	}
	// todo : filter block

	builder.pendingIndexHandle.InternalKey = internalKey

	builder.numEntries++
	builder.dataBlockBuilder.Add(internalKey)
	if builder.dataBlockBuilder.CurrentSizeEstimate() > MaxBlockSize {
		builder.flush()
	}
}

func (builder *TableBuilder) flush() {
	if builder.dataBlockBuilder.Empty() {
		return
	}
	orgKey := builder.pendingIndexHandle.InternalKey
	builder.pendingIndexHandle.InternalKey = internalkey.NewInternalKey(orgKey.Seq, orgKey.Type, orgKey.UserKey, nil)
	builder.pendingIndexHandle.SetBlockHandle(builder.writeBlock(&builder.dataBlockBuilder))
	builder.pendingIndexEntry = true
}

func (builder *TableBuilder) Finish() error {
	// write data block
	builder.flush()
	// todo : filter block

	// write index block
	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.Add(builder.pendingIndexHandle.InternalKey)
		builder.pendingIndexEntry = false
	}
	var footer Footer
	footer.IndexHandle = builder.writeBlock(&builder.indexBlockBuilder)

	// write footer block
	footer.EncodeTo(builder.file)
	builder.file.Close()
	return nil
}

func (builder *TableBuilder) writeBlock(blockBuilder *block.Builder) BlockHandle {
	content := blockBuilder.Finish()
	// snappy compress
	content = snappy.Encode(nil, content)
	var blockHandle BlockHandle
	blockHandle.Offset = builder.offset
	blockHandle.Size = uint32(len(content))
	builder.offset += uint32(len(content))

	go func() {
		err := syscall.Flock(int(builder.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err != nil {
			log.Panicf("flock failed: %s", err)
		}
		defer syscall.Flock(int(builder.file.Fd()), syscall.LOCK_UN)

		_, builder.status = builder.file.Write(content)
		builder.file.Sync()
	}()

	blockBuilder.Reset()
	return blockHandle
}
