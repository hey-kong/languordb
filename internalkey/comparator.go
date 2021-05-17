package internalkey

import "bytes"

func InternalKeyComparator(a, b interface{}) int {
	// Order by:
	//    increasing user internalkey (according to user-supplied comparator)
	//    decreasing sequence number
	//    decreasing type (though sequence# should be enough to disambiguate)
	aKey := a.(*InternalKey)
	bKey := b.(*InternalKey)
	if aKey == nil && bKey == nil {
		return 0
	} else if aKey == nil {
		return -1
	} else if bKey == nil {
		return 1
	}
	r := UserKeyComparator(aKey.UserKey, bKey.UserKey)
	if r == 0 {
		anum := aKey.Seq
		bnum := bKey.Seq
		if anum > bnum {
			r = -1
		} else if anum < bnum {
			r = +1
		}
	}
	return r
}

func UserKeyComparator(a, b interface{}) int {
	aKey := a.([]byte)
	bKey := b.([]byte)
	return bytes.Compare(aKey, bKey)
}
