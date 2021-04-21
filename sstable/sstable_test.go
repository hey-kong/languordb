package sstable

import (
	"fmt"
	"testing"

	"github.com/hey-kong/languordb/internalkey"
)

func TestSSTable(t *testing.T) {
	builder := NewTableBuilder("D:\\languordb\\test_sstable.ldb")
	item := internalkey.NewInternalKey(1, internalkey.TypeValue, []byte("111"), []byte("aaa"))
	builder.Add(item)
	item = internalkey.NewInternalKey(2, internalkey.TypeValue, []byte("222"), []byte("bbb"))
	builder.Add(item)
	item = internalkey.NewInternalKey(3, internalkey.TypeValue, []byte("333"), []byte("ccc"))
	builder.Add(item)
	builder.Finish()

	table, err := Open("D:\\languordb\\test_sstable.ldb")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(table.index)
		fmt.Println(table.footer)
	}

	it := table.NewIterator()
	it.Seek([]byte("222"))
	if it.Valid() {
		if string(it.InternalKey().UserValue) != "bbb" {
			t.Fail()
		}
	} else {
		t.Fail()
	}
	it.Seek([]byte("2222"))
	if it.Valid() {
		if string(it.InternalKey().UserKey) != "333" {
			t.Fail()
		}
	} else {
		t.Fail()
	}
}
