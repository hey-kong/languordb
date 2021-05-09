package languordb

import (
	"log"

	"github.com/hey-kong/languordb/config"
	"github.com/hey-kong/languordb/memtable"
	"github.com/hey-kong/languordb/sstable"
	"github.com/hey-kong/languordb/utils"
)

// CgCompaction is initialized before coarse-grain compaction works
type CgCompaction struct {
	level  int
	inputs []*Shard
}

func (c *CgCompaction) Log() {
	log.Printf("CgCompaction, level:%d", c.level)
	for i := range c.inputs {
		for j := range c.inputs[i].pages {
			log.Printf("inputs[0]: %d", c.inputs[i].pages[j].number)
		}
	}
}

func (v *Version) WriteCgLevel0Table(imm *memtable.MemTable) {
	var meta FileMetaData
	meta.allowSeeks = 1 << 30
	meta.number = v.nextFileNumber
	v.nextFileNumber++
	builder := sstable.NewTableBuilder(utils.TableFileName(v.tableCache.dbName, meta.number))
	iter := imm.NewIterator()
	iter.SeekToFirst()
	if iter.Valid() {
		if config.RowCache {
			// use goroutine to update cache
			go func() {
				cacheIter := imm.NewIterator()
				cacheIter.SeekToFirst()
				for ; cacheIter.Valid(); cacheIter.Next() {
					v.rowCache.Add(cacheIter.InternalKey().UserKey, cacheIter.InternalKey().UserValue)
				}
			}()
		}
		// write to sstable
		meta.smallest = iter.InternalKey()
		for ; iter.Valid(); iter.Next() {
			meta.largest = iter.InternalKey()
			builder.Add(iter.InternalKey())
		}
		builder.Finish()
		meta.fileSize = uint64(builder.FileSize())
		meta.smallest.UserValue = nil
		meta.largest.UserValue = nil
	}

	level := 0
	v.addFile(level, &meta)

	var shard Shard
	shard.smallest = meta.smallest
	shard.largest = meta.largest
	shard.fileSize = meta.fileSize
	shard.pages = []*FileMetaData{&meta}
	v.index[level].fileSize += shard.fileSize
	v.index[level].shards = append(v.index[level].shards, &shard)
}

func (v *Version) DoCgCompactionWork() bool {
	c := v.pickCgCompaction()
	if c == nil {
		return false
	}
	log.Printf("DoCgCompactionWork begin\n")
	defer log.Printf("DoCgCompactionWork end\n")
	c.Log()
	shard := v.MergeShards(c.inputs)
	// Update Level-i index
	for i := range c.inputs {
		for j := range c.inputs[i].pages {
			v.deleteFile(c.level, c.inputs[i].pages[j])
		}
	}
	v.index[c.level].fileSize = 0
	v.index[c.level].shards = v.index[c.level].shards[:0]
	// Update Level-i+1 index
	for i := 0; i < len(shard.pages); i++ {
		v.addFile(c.level+1, shard.pages[i])
	}
	v.index[c.level+1].fileSize += shard.fileSize
	v.index[c.level+1].shards = append(v.index[c.level+1].shards, shard)
	return true
}

func (v *Version) pickCgCompaction() *CgCompaction {
	var c CgCompaction
	c.level = v.pickCgCompactionLevel()
	if c.level < 0 {
		return nil
	}
	// Shards may overlap each other, so pick up all
	c.inputs = append(c.inputs, v.index[c.level].shards...)
	return &c
}

func (v *Version) pickCgCompactionLevel() int {
	compactionLevel := -1
	bestScore := 1.0
	score := 0.0
	for level := 0; level < config.NumLevels-1; level++ {
		score = float64(len(v.index[level].shards)) / float64(config.MaxLevelShards)
		if score > bestScore {
			bestScore = score
			compactionLevel = level
		}
	}
	return compactionLevel
}

func (v *Version) cgMakeInputIterator(c *CgCompaction) *MergingIterator {
	var list []*sstable.Iterator
	for i := range c.inputs {
		for j := range c.inputs[i].pages {
			list = append(list, v.tableCache.NewIterator(c.inputs[i].pages[j].number))
		}
	}
	return NewMergingIterator(list)
}
