package skiplist

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math"
	"sort"
	"testing"
)

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func compare(a, b interface{}) int {
	var x int32
	var y int32

	if a != nil {
		x = a.(int32)
	}
	if b != nil {
		y = b.(int32)
	}

	if x > y {
		return +1
	} else if x < y {
		return -1
	}
	return 0
}

func min(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func TestInsertAndLookup(t *testing.T) {

	fmt.Println("Test: insert and lookup")

	const (
		N = 2000
		R = 5000
	)
	keys := make(map[int32]int)
	kvs := make(map[int32]string)
	var minKey int32 = math.MaxInt32

	rnd := NewRandom(1000)
	list := NewSkipList(compare)
	for i := 0; i < N; i++ {
		key := int32(rnd.Next() % R)
		value := randstring(5)
		keys[key]++
		if count := keys[key]; count == 1 {
			kvs[key] = value
			minKey = min(minKey, key)
			list.Insert(key, value)
		}
	}

	for key := range keys {
		if !list.Contains(key) {
			t.Fail()
		}
		x := list.Lookup(key)
		if x == nil || x != kvs[key] {
			t.Fail()
		}
	}

	// Simple iterator tests
	var iter *Iterator
	iter = NewIterator(list)
	if iter.Valid() {
		t.Fail()
	}

	iter.Seek(int32(0))
	if !iter.Valid() {
		t.Fail()
	}
	if iter.Value() != kvs[minKey] {
		t.Fail()
	}

	iter.SeekToFirst()
	if iter.Value() != kvs[minKey] {
		t.Fail()
	}

	// Forward iteration test
	sortedKeys := make([]int, 0)
	for key := range keys {
		sortedKeys = append(sortedKeys, int(key))
	}
	sort.Ints(sortedKeys)

	iter.SeekToFirst()
	for _, key := range sortedKeys {
		if !iter.Valid() {
			t.Fail()
		}
		if iter.Key() != int32(key) || iter.Value() != kvs[int32(key)] {
			t.Fail()
		}
		iter.Next()
	}
	if iter.Valid() {
		t.Fail()
	}

	// Backward iteration test
	iter.SeekToLast()
	for i := len(sortedKeys) - 1; i >= 0; i-- {
		if !iter.Valid() {
			t.Fail()
		}
		key := int32(sortedKeys[i])
		if iter.Key() != key || iter.Value() != kvs[key] {
			t.Fail()
		}
		iter.Prev()
	}
	if iter.Valid() {
		t.Fail()
	}
}
