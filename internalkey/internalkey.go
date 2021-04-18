package internalkey

import (
	"encoding/binary"
	"io"
	"math"
)

type ValueType int8

const (
	TypeDeletion ValueType = 0
	TypeValue    ValueType = 1
)

type InternalKey struct {
	Seq       uint64
	Type      ValueType
	UserKey   []byte
	UserValue []byte
}

func NewInternalKey(seq uint64, valueType ValueType, key, value []byte) *InternalKey {
	var internalKey InternalKey
	internalKey.Seq = seq
	internalKey.Type = valueType

	internalKey.UserKey = make([]byte, len(key))
	copy(internalKey.UserKey, key)
	internalKey.UserValue = make([]byte, len(value))
	copy(internalKey.UserValue, value)

	return &internalKey
}

func (key *InternalKey) EncodeTo(w io.Writer) error {
	var isNil bool
	if key == nil {
		isNil = true
		return binary.Write(w, binary.LittleEndian, isNil)
	}

	binary.Write(w, binary.LittleEndian, isNil)
	binary.Write(w, binary.LittleEndian, key.Seq)
	binary.Write(w, binary.LittleEndian, key.Type)
	binary.Write(w, binary.LittleEndian, int32(len(key.UserKey)))
	binary.Write(w, binary.LittleEndian, key.UserKey)
	binary.Write(w, binary.LittleEndian, int32(len(key.UserValue)))
	return binary.Write(w, binary.LittleEndian, key.UserValue)
}

func (key *InternalKey) DecodeFrom(r io.Reader) error {
	var isNil bool
	binary.Read(r, binary.LittleEndian, &isNil)
	if isNil {
		key = nil
		return nil
	}

	var tmp int32
	binary.Read(r, binary.LittleEndian, &key.Seq)
	binary.Read(r, binary.LittleEndian, &key.Type)
	binary.Read(r, binary.LittleEndian, &tmp)
	key.UserKey = make([]byte, tmp)
	binary.Read(r, binary.LittleEndian, key.UserKey)
	binary.Read(r, binary.LittleEndian, &tmp)
	key.UserValue = make([]byte, tmp)
	return binary.Read(r, binary.LittleEndian, key.UserValue)
}

func LookupKey(key []byte) *InternalKey {
	return NewInternalKey(math.MaxUint64, TypeValue, key, nil)
}
