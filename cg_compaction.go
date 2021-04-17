package languorDB

import (
	"LanguorDB/internalkey"
	"log"

	"LanguorDB/config"
	"LanguorDB/sstable"
)

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

func (v *Version) DoCgCompactionWork() bool {
	c := v.pickCgCompaction()
	if c == nil {
		return false
	}
	log.Printf("DoCgCompactionWork begin\n")
	defer log.Printf("DoCgCompactionWork end\n")
	c.Log()
	s := v.MergeShards(c.inputs)
	// Update Level-i index
	for i := range c.inputs {
		for j := range c.inputs[i].pages {
			v.deleteFile(c.level, c.inputs[i].pages[j])
		}
	}
	v.index[c.level].smallest = nil
	v.index[c.level].largest = nil
	v.index[c.level].fileSize = 0
	v.index[c.level].shards = v.index[c.level].shards[:0]
	// Update Level-i+1 index
	for i := 0; i < len(s.pages); i++ {
		v.addFile(c.level+1, s.pages[i])
	}
	if internalkey.InternalKeyComparator(v.index[c.level+1].smallest, s.smallest) > 0 {
		v.index[c.level+1].smallest = s.smallest
	}
	if internalkey.InternalKeyComparator(v.index[c.level+1].largest, s.largest) < 0 {
		v.index[c.level+1].largest = s.largest
	}
	v.index[c.level+1].fileSize += s.fileSize
	v.index[c.level+1].shards = append(v.index[c.level+1].shards, s)
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
