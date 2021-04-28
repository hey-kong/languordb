package skiplist

import (
	"sync/atomic"
)

const (
	MaxHeight = 12
	Branching = 4
)

type KeyComparator func(a interface{}, b interface{}) int

type SkipList struct {
	head      *Node
	maxHeight int32
	rnd       *Random
	compare   KeyComparator
}

func NewSkipList(compare KeyComparator) *SkipList {
	list := &SkipList{
		head:      NewNode(nil, nil, MaxHeight),
		maxHeight: 1,
		rnd:       NewRandom(0xdeadbeef),
		compare:   compare,
	}
	for i := int32(0); i < MaxHeight; i++ {
		list.head.SetNext(i, nil)
	}
	return list
}

func (l *SkipList) randomHeight() int32 {
	var height int32 = 1
	for height < MaxHeight && (l.rnd.Next()%Branching) == 0 {
		height++
	}
	if height <= 0 || height > MaxHeight {
		panic("")
	}
	return height
}

func (l *SkipList) getMaxHeight() int32 {
	return atomic.LoadInt32(&l.maxHeight)
}

func (l *SkipList) keyIsAfterNode(key interface{}, n *Node) bool {
	return (n != nil) && (l.compare(n.key, key) < 0)
}

func (l *SkipList) findGreaterOrEqual(key interface{}, prev []*Node) *Node {
	var x *Node
	x = l.head
	level := l.getMaxHeight() - 1
	for {
		next := x.Next(level)
		if l.keyIsAfterNode(key, next) {
			// Keep searching in this list
			x = next
		} else {
			if prev != nil {
				prev[level] = x
			}
			if level == 0 {
				return next
			} else {
				// Switch to next list
				level--
			}
		}
	}
}

func (l *SkipList) findLessThan(key interface{}) *Node {
	var x *Node
	x = l.head
	level := l.getMaxHeight() - 1
	for {
		if (x != l.head) && (l.compare(x.key, key) >= 0) {
			panic("")
		}
		next := x.Next(level)
		if next == nil || l.compare(next.key, key) >= 0 {
			if level == 0 {
				return x
			} else {
				// Switch to next list
				level--
			}
		} else {
			x = next
		}
	}
}

func (l *SkipList) findLast() *Node {
	var x *Node
	x = l.head
	level := l.getMaxHeight() - 1
	for {
		next := x.Next(level)
		if next == nil {
			if level == 0 {
				return x
			} else {
				// Switch to next list
				level--
			}
		} else {
			x = next
		}
	}
}

func (l *SkipList) Contains(key interface{}) bool {
	x := l.findGreaterOrEqual(key, nil)
	if x != nil && l.compare(key, x.key) == 0 {
		return true
	}
	return false
}

func (l *SkipList) Lookup(key interface{}) interface{} {
	x := l.findGreaterOrEqual(key, nil)
	if x != nil && l.compare(x.key, key) == 0 {
		return x.value
	}
	return nil
}

func (l *SkipList) Insert(key interface{}, value interface{}) {
	var x *Node
	prev := make([]*Node, MaxHeight)
	x = l.findGreaterOrEqual(key, prev)

	// Our data structure does not allow duplicate insertion
	if x != nil && l.compare(key, x.key) == 0 {
		panic("duplicate insertion")
	}

	height := l.randomHeight()
	if height > l.getMaxHeight() {
		for i := l.getMaxHeight(); i < height; i++ {
			prev[i] = l.head
		}
		atomic.StoreInt32(&l.maxHeight, height)
	}

	x = NewNode(key, value, height)
	for i := int32(0); i < height; i++ {
		x.SetNext(i, prev[i].Next(i))
		prev[i].SetNext(i, x)
	}
}

type Node struct {
	key   interface{}
	value interface{}
	next  []*Node
}

func NewNode(key interface{}, value interface{}, height int32) *Node {
	node := &Node{
		key:   key,
		value: value,
		next:  make([]*Node, height),
	}
	return node
}

func (n *Node) Next(i int32) *Node {
	if i < 0 || i >= MaxHeight {
		panic("out of bound")
	}

	// The following code is unsafe
	// x := atomic.LoadUintptr((*uintptr)(unsafe.Pointer(&n.next[i])))
	// y := (*Node)(unsafe.Pointer(x))
	// return y

	return n.next[i]
}

func (n *Node) SetNext(i int32, x *Node) {
	if i < 0 || i >= MaxHeight {
		panic("out of bound")
	}

	// The following code is unsafe
	// atomic.StoreUintptr((*uintptr)(unsafe.Pointer(&n.next[i])), (uintptr)(unsafe.Pointer(x)))

	n.next[i] = x
}

type Iterator struct {
	list *SkipList
	node *Node
}

func NewIterator(list *SkipList) *Iterator {
	iter := &Iterator{
		list: list,
		node: nil,
	}
	return iter
}

func (iter *Iterator) Valid() bool {
	return iter.node != nil
}

func (iter *Iterator) Key() interface{} {
	if !iter.Valid() {
		panic("invalid")
	}
	return iter.node.key
}

func (iter *Iterator) Value() interface{} {
	if !iter.Valid() {
		panic("invalid")
	}
	return iter.node.value
}

func (iter *Iterator) Next() {
	if !iter.Valid() {
		panic("invalid")
	}
	iter.node = iter.node.Next(0)
}

func (iter *Iterator) Prev() {
	if !iter.Valid() {
		panic("invalid")
	}
	iter.node = iter.list.findLessThan(iter.node.key)
	if iter.node == iter.list.head {
		iter.node = nil
	}
}

func (iter *Iterator) Seek(target interface{}) {
	iter.node = iter.list.findGreaterOrEqual(target, nil)
}

func (iter *Iterator) SeekToFirst() {
	iter.node = iter.list.head.Next(0)
}

func (iter *Iterator) SeekToLast() {
	iter.node = iter.list.findLast()
	if iter.node == iter.list.head {
		iter.node = nil
	}
}
