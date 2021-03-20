package memtable

import (
	"fmt"
	"testing"

	"LanguorDB/internalkey"
)

func Test_MemTable(t *testing.T) {
	memTable := New()
	memTable.Add(1234567, internalkey.TypeValue, []byte("aadsa34a"), []byte("bb23b3423"))
	value, _ := memTable.Get([]byte("aadsa34a"))
	fmt.Println(string(value))
	fmt.Println(memTable.ApproximateMemoryUsage())
}
