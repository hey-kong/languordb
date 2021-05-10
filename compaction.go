package languordb

import (
	"log"

	"github.com/hey-kong/languordb/config"
	"github.com/hey-kong/languordb/memtable"
	"github.com/hey-kong/languordb/sstable"
	"github.com/hey-kong/languordb/utils"
)

// Compaction is initialized before coarse-grain compaction works
type Compaction struct {
	level  int
	inputs []*Shard
}

func (v *Version) deleteFile(level int, meta *FileMetaData) {
	numFiles := len(v.files[level])
	for i := 0; i < numFiles; i++ {
		if v.files[level][i].number == meta.number {
			v.files[level] = append(v.files[level][:i], v.files[level][i+1:]...)
			log.Printf("deleteFile, level:%d, num:%d", level, meta.number)
			break
		}
	}
}

func (v *Version) addFile(level int, meta *FileMetaData) {
	log.Printf("addFile, level:%d, num:%d, %s-%s", level, meta.number, string(meta.smallest.UserKey), string(meta.largest.UserKey))
	if level == 0 {
		// 0层没有排序
		v.files[level] = append(v.files[level], meta)
	} else {
		numFiles := len(v.files[level])
		index := v.findFile(v.files[level], meta.smallest.UserKey)
		if index >= numFiles {
			v.files[level] = append(v.files[level], meta)
		} else {
			var tmp []*FileMetaData
			tmp = append(tmp, v.files[level][:index]...)
			tmp = append(tmp, meta)
			v.files[level] = append(tmp, v.files[level][index:]...)
		}
	}
}

func (c *Compaction) Log() {
	log.Printf("coarse-grain compaction, level:%d", c.level)
	for i := range c.inputs {
		numbers := make([]uint64, len(c.inputs[i].pages))
		for j := range c.inputs[i].pages {
			numbers[j] = c.inputs[i].pages[j].number
		}
		log.Printf("inputs[%d], file number%v", i, numbers)
	}
}

func (v *Version) WriteLevel0Table(imm *memtable.MemTable) {
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

func (v *Version) DoCompactionWork() bool {
	c := v.pickCompaction()
	if c == nil {
		return false
	}
	log.Printf("DoCompactionWork begin\n")
	defer log.Printf("DoCompactionWork end\n")
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

func (v *Version) pickCompaction() *Compaction {
	var c Compaction
	c.level = v.pickCompactionLevel()
	if c.level < 0 {
		return nil
	}
	// Shards may overlap each other, so pick up all
	c.inputs = append(c.inputs, v.index[c.level].shards...)
	return &c
}

func (v *Version) pickCompactionLevel() int {
	compactionLevel := -1
	bestScore := 1.0
	score := 0.0
	for level := 0; level < config.NumLevels-1; level++ {
		score = float64(len(v.index[level].shards)) / float64(config.MaxLevelShards)
		if score >= bestScore {
			bestScore = score
			compactionLevel = level
		}
	}
	return compactionLevel
}
