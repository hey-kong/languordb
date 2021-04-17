package languorDB

import (
	"encoding/binary"
	"io"
	"sort"
	"sync"

	"LanguorDB/config"
	"LanguorDB/errors"
	"LanguorDB/internalkey"
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

func (v *Version) ParallelGet(key []byte) ([]byte, error) {
	var tmp []*FileMetaData
	var files []*FileMetaData
	// We can search level-by-level since entries never hop across
	// levels.  Therefore we are guaranteed that if we find data
	// in an smaller level, later levels are irrelevant.
	for level := 0; level < config.NumLevels; level++ {
		numShards := len(v.index[level].shards)
		if numShards == 0 {
			continue
		}

		for i := 0; i < numShards; i++ {
			for j := range v.index[level].shards[i].pages {
				f := v.index[level].shards[i].pages[j]
				if internalkey.UserKeyComparator(key, f.smallest.UserKey) >= 0 && internalkey.UserKeyComparator(key, f.largest.UserKey) <= 0 {
					tmp = append(tmp, f)
					break
				}
			}
		}
		if len(tmp) == 0 {
			continue
		}
		sort.Slice(tmp, func(i, j int) bool { return tmp[i].number > tmp[j].number })
		numFiles := len(tmp)
		files = tmp

		var wg sync.WaitGroup
		var mu sync.Mutex
		var res []byte = nil
		var resErr error = errors.ErrNotFound
		var resFileNum uint64 = 0
		for i := 0; i < numFiles; i++ {
			f := files[i]
			wg.Add(1)
			go func() {
				defer wg.Done()
				value, err := v.tableCache.Get(f.number, key)
				mu.Lock()
				if err != errors.ErrNotFound && f.number >= resFileNum {
					res = value
					resErr = err
					resFileNum = f.number
				}
				mu.Unlock()
			}()
		}
		wg.Wait()
		if resErr != errors.ErrNotFound {
			return res, resErr
		}
	}
	return nil, errors.ErrNotFound
}
