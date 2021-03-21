package index

import (
	"LanguorDB/errors"
	"LanguorDB/internalkey"
	"LanguorDB/shard"
)

type Index struct {
	min    *internalkey.InternalKey
	max    *internalkey.InternalKey
	num    int
	shards []*shard.Shard
}

func (index *Index) Get(key []byte) ([]byte, error) {
	// traverse shards
	return nil, errors.ErrNotFound
}
