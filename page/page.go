package page

import "LanguorDB/internalkey"

type MetaData struct {
	allowSeeks uint64
	number     uint64
	fileSize   uint64
	smallest   *internalkey.InternalKey
	largest    *internalkey.InternalKey
}

// TODO: SSTable -> Page
type Page struct {
}
