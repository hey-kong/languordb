package languordb

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/hey-kong/languordb/util"
)

const WalBlockSize = 32 * 1024

func (db *DB) SetWalFile() {
	walFile := util.WalFileName(db.name)
	f, err := os.OpenFile(walFile, os.O_RDWR|os.O_TRUNC|os.O_CREATE|os.O_SYNC, 0600)
	if err != nil {
		panic(err)
	}
	db.file = f
}

func (db *DB) SetLogFile() {
	logFile := util.LogFileName(db.name)
	if exist, err := util.IsExist(logFile); err == nil {
		if exist {
			oldFile := fmt.Sprintf("%s.%s", logFile, "old")
			os.Rename(logFile, oldFile)
		}
	} else {
		panic(err)
	}

	f, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		panic(err)
	}
	log.SetOutput(f)
	log.SetFlags(log.LstdFlags | log.LUTC)
}

func (db *DB) SetCurrentFile(descriptorNumber uint64) {
	tmpFile := util.TempFileName(db.name, descriptorNumber)
	ioutil.WriteFile(tmpFile, []byte(fmt.Sprintf("%d", descriptorNumber)), 0600)
	os.Rename(tmpFile, util.CurrentFileName(db.name))
}

func (db *DB) ReadCurrentFile() uint64 {
	b, err := ioutil.ReadFile(util.CurrentFileName(db.name))
	if err != nil {
		return 0
	}
	descriptorNumber, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0
	}
	return descriptorNumber
}

// AddRecord simulates write-ahead logging
func (db *DB) AddRecord(record []byte) error {
	f, err := db.file.Stat()
	if err != nil {
		return err
	}
	if f.Size() > WalBlockSize {
		db.file.Truncate(0)
	}

	n, _ := db.file.Seek(0, io.SeekEnd)
	_, err = db.file.WriteAt(record, n)
	return err
}
