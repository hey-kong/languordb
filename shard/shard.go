package shard

import (
	"LanguorDB/internalkey"
	"LanguorDB/page"
)

type Shard struct {
	min   *internalkey.InternalKey
	max   *internalkey.InternalKey
	num   int
	pages []*page.MetaData
}
