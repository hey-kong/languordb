package languorDB

import (
	"encoding/binary"
	"io"

	"LanguorDB/errors"
)

type Index struct {
	fileSize uint64
	shards   []*Shard
}

func NewIndex() *Index {
	var index Index
	index.fileSize = 0
	index.shards = []*Shard{}
	return &index
}

func (index *Index) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, index.fileSize)
	numShards := len(index.shards)
	binary.Write(w, binary.LittleEndian, int32(numShards))
	for i := range index.shards {
		index.shards[i].EncodeTo(w)
	}
	return nil
}

func (index *Index) DecodeFrom(r io.Reader) error {
	binary.Read(r, binary.LittleEndian, &index.fileSize)
	var numShards int32
	binary.Read(r, binary.LittleEndian, &numShards)
	index.shards = make([]*Shard, numShards)
	for i := 0; i < int(numShards); i++ {
		var shard Shard
		shard.DecodeFrom(r)
		index.shards[i] = &shard
	}
	return nil
}

func (v *Version) MultiGet(key []byte) ([]byte, error) {
	return nil, errors.ErrNotFound
}
