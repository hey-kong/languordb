package languorDB

import (
	"sync"
	"time"

	"LanguorDB/config"
	"LanguorDB/errors"
	"LanguorDB/internalkey"
	"LanguorDB/memtable"
)

type Db struct {
	name                  string
	mu                    sync.Mutex
	cond                  *sync.Cond
	mem                   *memtable.MemTable
	imm                   *memtable.MemTable
	current               *Version
	bgCompactionScheduled bool
}

func Open(dbName string) *Db {
	var db Db
	db.name = dbName
	db.mem = memtable.New()
	db.imm = nil
	db.bgCompactionScheduled = false
	db.cond = sync.NewCond(&db.mu)
	num := db.ReadCurrentFile()
	if num > 0 {
		v, err := Load(dbName, num)
		if err != nil {
			return nil
		}
		db.current = v
	} else {
		db.current = New(dbName)
	}

	return &db
}

func (db *Db) Close() {
	db.mu.Lock()
	for db.bgCompactionScheduled {
		db.cond.Wait()
	}
	db.mu.Unlock()
}

func (db *Db) Put(key, value []byte) error {
	// May temporarily unlock and wait.
	seq, err := db.makeRoomForWrite()
	if err != nil {
		return err
	}

	// todo : add log

	db.mem.Add(seq, internalkey.TypeValue, key, value)
	return nil
}

func (db *Db) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	mem := db.mem
	imm := db.mem
	current := db.current
	db.mu.Unlock()
	value, err := mem.Get(key)
	if err != errors.ErrNotFound {
		return value, err
	}

	if imm != nil {
		value, err := imm.Get(key)
		if err != errors.ErrNotFound {
			return value, err
		}
	}

	if config.RowCache {
		value, err := db.current.rowCache.Get(key)
		if err != errors.ErrNotFound {
			return value, err
		}
	}

	value, err = current.ParallelGet(key)
	if config.RowCache && err == nil {
		db.current.rowCache.Add(key, value)
	}
	return value, err
}

func (db *Db) Delete(key []byte) error {
	seq, err := db.makeRoomForWrite()
	if err != nil {
		return err
	}
	db.mem.Add(seq, internalkey.TypeDeletion, key, nil)
	return nil
}

func (db *Db) makeRoomForWrite() (uint64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	for true {
		if db.current.NumLevelFiles(0) >= config.L0_SlowdownWritesTrigger {
			db.mu.Unlock()
			time.Sleep(time.Duration(1000) * time.Microsecond)
			db.mu.Lock()
		} else if db.mem.ApproximateMemoryUsage() <= config.WriteBufferSize {
			return db.current.NextSeq(), nil
		} else if db.imm != nil {
			//  Current memtable full; waiting
			db.cond.Wait()
		} else {
			// Attempt to switch to a new memtable and trigger compaction of old
			// todo : switch log
			db.imm = db.mem
			db.mem = memtable.New()
			db.maybeScheduleCompaction()
		}
	}

	return db.current.NextSeq(), nil
}
