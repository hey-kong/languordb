package languorDB

import (
	"log"
	"sort"

	"LanguorDB/config"
	"LanguorDB/internalkey"
	"LanguorDB/sstable"
	"LanguorDB/utils"
)

type Shard struct {
	smallest *internalkey.InternalKey
	largest  *internalkey.InternalKey
	fileSize uint64
	pages    []*FileMetaData
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
		builder := sstable.NewTableBuilder((utils.TableFileName(v.tableCache.dbName, meta.number)))

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
				currentKey = iter.InternalKey()
			}
			meta.largest = iter.InternalKey()
			builder.Add(iter.InternalKey())
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
	s := &Shard{}
	for i := 1; i < len(list); i++ {
		s.fileSize += list[i].fileSize
		if internalkey.InternalKeyComparator(s.largest, list[i].largest) < 0 {
			s.largest = list[i].largest
		}
		if internalkey.InternalKeyComparator(s.smallest, list[i].smallest) > 0 {
			s.smallest = list[i].smallest
		}
	}
	s.pages = list
	return s
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
	sort.Sort(Metas(metas))
	fileSet := make(map[uint64]bool)
	for i := range metas {
		if internalkey.UserKeyComparator(metas[i].largest.UserKey, metas[i+1].smallest.UserKey) < 0 {
			fileSet[metas[i].number] = true
		}
	}
	return fileSet
}
