package memtable

import (
	"fmt"
	"testing"

	"github.com/hey-kong/languordb/internalkey"
)

func TestMemTable(t *testing.T) {
	memTable := New()
	memTable.Add(123, internalkey.TypeValue, []byte("193"), []byte("Iggie Wang"))
	value, _ := memTable.Get([]byte("193"))

	if string(value) != "Iggie Wang" {
		t.Fail()
	}
	fmt.Println("MemoryUsage: ", memTable.ApproximateMemoryUsage())
}
