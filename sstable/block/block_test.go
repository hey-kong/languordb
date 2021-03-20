package block

import (
	"testing"

	"LanguorDB/internalkey"
)

func Test_SsTable(t *testing.T) {
	var builder BlockBuilder

	item := internalkey.NewInternalKey(1, internalkey.TypeValue, []byte("123"), []byte("1234"))
	builder.Add(item)
	item = internalkey.NewInternalKey(2, internalkey.TypeValue, []byte("124"), []byte("1245"))
	builder.Add(item)
	item = internalkey.NewInternalKey(3, internalkey.TypeValue, []byte("125"), []byte("0245"))
	builder.Add(item)
	p := builder.Finish()

	block := New(p)
	it := block.NewIterator()

	it.Seek([]byte("1244"))
	if it.Valid() {
		if string(it.InternalKey().UserKey) != "125" {
			t.Fail()
		}

	} else {
		t.Fail()
	}
}
