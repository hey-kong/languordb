package block

import (
	"testing"

	"LanguorDB/internalkey"
)

func TestBlock(t *testing.T) {
	var builder Builder
	item := internalkey.NewInternalKey(1, internalkey.TypeValue, []byte("111"), []byte("aaa"))
	builder.Add(item)
	item = internalkey.NewInternalKey(2, internalkey.TypeValue, []byte("222"), []byte("bbb"))
	builder.Add(item)
	item = internalkey.NewInternalKey(3, internalkey.TypeValue, []byte("333"), []byte("ccc"))
	builder.Add(item)
	p := builder.Finish()

	block := New(p)

	it := block.NewIterator()
	it.Seek([]byte("2222"))
	if it.Valid() {
		if string(it.InternalKey().UserKey) != "333" {
			t.Fail()
		}
	} else {
		t.Fail()
	}
}
