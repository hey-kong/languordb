package skiplist

import (
	"fmt"
	"math/rand"
	"testing"
)

func IntComparator(a, b interface{}) int {
	aInt := a.(int)
	bInt := b.(int)
	return aInt - bInt
}

func Test_Insert(t *testing.T) {
	skiplist := New(IntComparator)
	for i := 0; i < 10; i++ {
		skiplist.Insert(rand.Int() % 10)
	}
	it := skiplist.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		fmt.Println(it.Key())
	}
	fmt.Println()
	for it.SeekToLast(); it.Valid(); it.Prev() {
		fmt.Println(it.Key())
	}
}
