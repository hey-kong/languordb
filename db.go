package languordb

import (
	"log"
	"sync"
	"time"

	"github.com/hey-kong/languordb/config"
	"github.com/hey-kong/languordb/errors"
	"github.com/hey-kong/languordb/internalkey"
	"github.com/hey-kong/languordb/memtable"
)

type DB struct {
	name                  string
	mu                    sync.Mutex
	cond                  *sync.Cond
	mem                   *memtable.MemTable
	imm                   *memtable.MemTable
	current               *Version
	bgCompactionScheduled bool
}

func Open(dbName string) (*DB, error) {
	var db DB
	db.name = dbName
	db.mem = memtable.New()
	db.imm = nil
	db.bgCompactionScheduled = false
	db.cond = sync.NewCond(&db.mu)
	num := db.ReadCurrentFile()
	if num > 0 {
		v, err := Load(dbName, num)
		if err != nil {
			return nil, err
		}
		db.current = v
	} else {
		db.current = New(dbName)
	}

	return &db, nil
}

func (db *DB) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()
	for db.bgCompactionScheduled {
		db.cond.Wait()
	}
}

func (db *DB) Put(key, value []byte) error {
	// May temporarily unlock and wait.
	seq, err := db.makeRoomForWrite()
	if err != nil {
		return err
	}

	// todo : add log

	db.mem.Add(seq, internalkey.TypeValue, key, value)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
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

func (db *DB) Delete(key []byte) error {
	seq, err := db.makeRoomForWrite()
	if err != nil {
		return err
	}
	db.mem.Add(seq, internalkey.TypeDeletion, key, nil)
	return nil
}

func (db *DB) makeRoomForWrite() (uint64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	allowDelay := true
	for true {
		if allowDelay && db.current.NumLevelFiles(0) >= config.L0_SlowdownWritesTrigger {
			db.mu.Unlock()
			time.Sleep(1 * time.Millisecond)
			db.mu.Lock()
			allowDelay = false
		} else if db.mem.ApproximateMemoryUsage() <= config.WriteBufferSize {
			break
		} else if db.imm != nil {
			log.Println("Current memtable full; waiting...")
			db.cond.Wait()
		} else if db.current.NumLevelFiles(0) >= config.L0_SlowdownWritesTrigger {
			log.Println("Too many L0 files; waiting...")
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
