package block

import (
	"bytes"
	"encoding/binary"

	"github.com/hey-kong/languordb/internalkey"
)

type Builder struct {
	buf     bytes.Buffer
	counter uint32
}

func (blockBuilder *Builder) Reset() {
	blockBuilder.counter = 0
	blockBuilder.buf.Reset()
}

func (blockBuilder *Builder) Add(item *internalkey.InternalKey) error {
	blockBuilder.counter++
	return item.EncodeTo(&blockBuilder.buf)
}

func (blockBuilder *Builder) Finish() []byte {
	binary.Write(&blockBuilder.buf, binary.LittleEndian, blockBuilder.counter)
	return blockBuilder.buf.Bytes()
}

func (blockBuilder *Builder) CurrentSizeEstimate() int {
	return blockBuilder.buf.Len()
}

func (blockBuilder *Builder) Empty() bool {
	return blockBuilder.buf.Len() == 0
}
