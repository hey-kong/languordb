package languordb

func (db *DB) maybeScheduleCompaction() {
	if db.bgCompactionScheduled {
		return
	}
	db.bgCompactionScheduled = true
	go db.backgroundCall()
}

func (db *DB) backgroundCall() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.backgroundCompaction()
	db.bgCompactionScheduled = false
	db.cond.Broadcast()
}

func (db *DB) backgroundCompaction() {
	imm := db.imm
	version := db.current.Copy()
	db.mu.Unlock()
	// minor compaction
	if imm != nil {
		version.WriteLevel0Table(imm)
	}
	// major compaction
	for version.DoCompactionWork() {
		version.Log()
	}
	descriptorNumber, _ := version.Save()
	db.SetCurrentFile(descriptorNumber)
	db.mu.Lock()
	db.imm = nil
	db.current = version
}
