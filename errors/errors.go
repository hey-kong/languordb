package errors

import (
	"errors"
)

var (
	ErrNotFound          = errors.New("not found")
	ErrDeletion          = errors.New("found deleted value")
	ErrTableFileMagic    = errors.New("not a sstable (bad magic number)")
	ErrTableFileTooShort = errors.New("file is too short to be a sstable")
)
