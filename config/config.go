package config

const (
	L0_SlowdownWritesTrigger = 8
	WriteBufferSize          = 4 << 20
	NumLevels                = 7
	MaxOpenFiles             = 1000
	NumNonTableCacheFiles    = 10
	MaxFileSize              = 2 << 20
	MaxLevelShards           = 4
	RowCache                 = true
	RowCacheSize             = 100000
)
