package languorDB

import (
	"log"
)

type CgCompaction struct {
	level  int
	inputs [2][]*Shard
}

func (c *CgCompaction) Log() {
	log.Printf("CgCompaction, level:%d", c.level)
	for i := 0; i < len(c.inputs[0]); i++ {
		log.Printf("inputs[0]: %d", c.inputs[0][i].number)
	}
	for i := 0; i < len(c.inputs[1]); i++ {
		log.Printf("inputs[1]: %d", c.inputs[1][i].number)
	}
}
