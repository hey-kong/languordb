package languorDB

import (
	"LanguorDB/errors"
	"LanguorDB/internalkey"
)

type Index struct {
	min       *internalkey.InternalKey
	max       *internalkey.InternalKey
	fileSize  uint64
	numShards int
	shards    []*Shard
}

func (index *Index) Get(key []byte) ([]byte, error) {
	// traverse shards
	return nil, errors.ErrNotFound
}

func (index *Index) mergeShards() *Shard {
	l := index.numShards
	if l == 0 {
		return nil
	}

	notify := make(chan bool)
	for l > 1 {
		for i := 0; i < l/2; i++ {
			go func() {
				index.shards[i] = mergeTwoShards(index.shards[i], index.shards[l-1-i])
				notify <- true
			}()
		}
		for i := 0; i < l/2; i++ {
			<-notify
		}
		l = (l + 1) / 2
	}

	return index.shards[0]
}

func mergeTwoShards(s1 *Shard, s2 *Shard) *Shard {
	if s1 == nil {
		return s2
	}
	if s2 == nil {
		return s1
	}

	s := &Shard{}

	return s
}
