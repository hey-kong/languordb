package config

const (
	L0_CompactionTrigger     = 4
	L0_SlowdownWritesTrigger = 8
	WriteBufferSize          = 4 << 20
	NumLevels                = 7
	MaxOpenFiles             = 1000
	NumNonTableCacheFiles    = 10
	MaxMemCompactLevel       = 2
	MaxFileSize              = 2 << 20
	MaxLevelShards           = 4
)
