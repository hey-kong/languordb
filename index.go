package languorDB

import (
	"LanguorDB/errors"
	"LanguorDB/internalkey"
)

type Index struct {
	smallest *internalkey.InternalKey
	largest  *internalkey.InternalKey
	fileSize uint64
	shards   []*Shard
}

func (v *Version) MultiGet(key []byte) ([]byte, error) {
	// traverse shards
	return nil, errors.ErrNotFound
}
