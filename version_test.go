package languordb

import (
	"fmt"
	"testing"

	"github.com/hey-kong/languordb/internalkey"
	"github.com/hey-kong/languordb/memtable"
)

func TestVersionGet(t *testing.T) {
	v := New("D:\\languordb")
	var f FileMetaData
	f.number = 123
	f.smallest = internalkey.NewInternalKey(1, internalkey.TypeValue, []byte("123"), nil)
	f.largest = internalkey.NewInternalKey(1, internalkey.TypeValue, []byte("125"), nil)
	v.files[0] = append(v.files[0], &f)

	value, err := v.Get([]byte("125"))
	fmt.Println(err, value)
}

func TestVersionLoad(t *testing.T) {
	v := New("D:\\languordb")
	memTable := memtable.New()
	memTable.Add(1234567, internalkey.TypeValue, []byte("aadsa34a"), []byte("bb23b3423"))
	v.WriteLevel0Table(memTable)
	n, _ := v.Save()
	fmt.Println(v)

	v2, _ := Load("D:\\languordb", n)
	fmt.Println(v2)

	value, err := v2.Get([]byte("aadsa34a"))
	fmt.Println(err, value)
}

func TestVersionParallelGet(t *testing.T) {
	v := New("D:\\languordb")
	memTable := memtable.New()
	memTable.Add(1234567, internalkey.TypeValue, []byte("whu193"), []byte("old-whu193"))
	v.WriteLevel0Table(memTable)
	memTable = memtable.New()
	memTable.Add(1234567, internalkey.TypeValue, []byte("whu193"), []byte("new-whu193"))
	v.WriteLevel0Table(memTable)
	n, _ := v.Save()
	fmt.Println(v)

	v2, _ := Load("D:\\languordb", n)
	fmt.Println(v2)

	value, err := v2.ParallelGet([]byte("whu193"))
	if err != nil || string(value) != "new-whu193" {
		t.Fail()
	}
}
