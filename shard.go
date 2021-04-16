package languorDB

import (
	"LanguorDB/internalkey"
)

type Shard struct {
	number   uint64
	min      *internalkey.InternalKey
	max      *internalkey.InternalKey
	fileSize uint64
	numPages int
	pages    []*FileMetaData
}
