package languordb

import (
	"encoding/binary"
	"io"
	"log"
	"sort"

	"github.com/hey-kong/languordb/config"
	"github.com/hey-kong/languordb/internalkey"
	"github.com/hey-kong/languordb/sstable"
	"github.com/hey-kong/languordb/utils"
)

type Shard struct {
	smallest *internalkey.InternalKey
	largest  *internalkey.InternalKey
	fileSize uint64
	pages    []*FileMetaData
}

func (shard *Shard) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, shard.fileSize)
	shard.smallest.EncodeTo(w)
	shard.largest.EncodeTo(w)
	numPages := len(shard.pages)
	binary.Write(w, binary.LittleEndian, int32(numPages))
	for i := range shard.pages {
		shard.pages[i].EncodeTo(w)
	}
	return nil
}

func (shard *Shard) DecodeFrom(r io.Reader) error {
	binary.Read(r, binary.LittleEndian, &shard.fileSize)
	shard.smallest = new(internalkey.InternalKey)
	shard.smallest.DecodeFrom(r)
	shard.largest = new(internalkey.InternalKey)
	shard.largest.DecodeFrom(r)
	var numPages int32
	binary.Read(r, binary.LittleEndian, &numPages)
	shard.pages = make([]*FileMetaData, numPages)
	for i := 0; i < int(numPages); i++ {
		var meta FileMetaData
		meta.DecodeFrom(r)
		shard.pages[i] = &meta
	}
	return nil
}

func (v *Version) MergeShards(shards []*Shard) *Shard {
	if len(shards) == 0 {
		return nil
	}
	if len(shards) == 1 {
		return shards[0]
	}

	list, iter := v.makeShardsIterator(shards)
	var currentKey *internalkey.InternalKey
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		var meta FileMetaData
		meta.allowSeeks = 1 << 30
		meta.number = v.nextFileNumber
		v.nextFileNumber++
		builder := sstable.NewTableBuilder(utils.TableFileName(v.tableCache.dbName, meta.number))

		meta.smallest = iter.InternalKey()
		for ; iter.Valid(); iter.Next() {
			if currentKey != nil {
				// Remove duplicate KVs
				ret := internalkey.UserKeyComparator(iter.InternalKey().UserKey, currentKey.UserKey)
				if ret == 0 {
					continue
				} else if ret < 0 {
					log.Fatalf("%s < %s", string(iter.InternalKey().UserKey), string(currentKey.UserKey))
				}
			}
			meta.largest = iter.InternalKey()
			builder.Add(iter.InternalKey())
			currentKey = iter.InternalKey()
			if builder.FileSize() > config.MaxFileSize {
				break
			}
		}
		builder.Finish()
		meta.fileSize = uint64(builder.FileSize())
		meta.smallest.UserValue = nil
		meta.largest.UserValue = nil

		list = append(list, &meta)
	}

	sort.Sort(Metas(list))
	var s Shard
	s.smallest = list[0].smallest
	s.largest = list[len(list)-1].largest
	for i := 0; i < len(list); i++ {
		s.fileSize += list[i].fileSize
	}
	s.pages = list
	return &s
}

func (v *Version) makeShardsIterator(shards []*Shard) ([]*FileMetaData, *MergingIterator) {
	var metas []*FileMetaData
	for i := range shards {
		metas = append(metas, shards[i].pages...)
	}
	notOverlap := makeNotOverlapFileMap(metas)

	metas = metas[:0]
	var list []*sstable.Iterator
	for i := range shards {
		for j := range shards[i].pages {
			if notOverlap[shards[i].pages[j].number] {
				// If the file doesn't overlap with others, it will be recorded directly without merging
				metas = append(metas, shards[i].pages[j])
			} else {
				// Generate file iterator
				list = append(list, v.tableCache.NewIterator(shards[i].pages[j].number))
			}
		}
	}
	return metas, NewMergingIterator(list)
}

func makeNotOverlapFileMap(metas []*FileMetaData) map[uint64]bool {
	if len(metas) == 0 {
		return nil
	}

	sort.Sort(Metas(metas))
	fileSet := make(map[uint64]bool)
	for i := range metas {
		isOverlap := false
		for left := 0; left < i; left++ {
			if internalkey.UserKeyComparator(metas[left].largest.UserKey, metas[i].smallest.UserKey) >= 0 {
				isOverlap = true
				break
			}
		}
		if isOverlap {
			continue
		}
		for right := i + 1; right < len(metas); right++ {
			if internalkey.UserKeyComparator(metas[i].largest.UserKey, metas[right].smallest.UserKey) >= 0 {
				isOverlap = true
				break
			}
		}
		if !isOverlap {
			fileSet[metas[i].number] = true
		}
	}
	return fileSet
}
