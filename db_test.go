package languorDB

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func GetRandomString(length int) []byte {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}

	for i := 0; i < length; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return result
}

func TestDbImpl(t *testing.T) {
	db := open("D:\\LanguorDB")
	db.Put([]byte("123"), []byte("456"))

	value, err := db.Get([]byte("123"))
	fmt.Println(string(value))

	db.Delete([]byte("123"))
	value, err = db.Get([]byte("123"))
	fmt.Println(err)

	db.Put([]byte("123"), []byte("789"))
	value, _ = db.Get([]byte("123"))
	fmt.Println(string(value))
	db.close()
}

func TestDbLoad(t *testing.T) {
	db := open("D:\\LanguorDB")
	db.Put([]byte("123"), []byte("456"))

	for i := 0; i < 1000000; i++ {
		db.Put(GetRandomString(10), GetRandomString(10))
	}
	value, err := db.Get([]byte("123"))
	fmt.Println("db:", err, string(value))
	db.close()

	db2 := open("D:\\LanguorDB")
	value, err = db2.Get([]byte("123"))
	fmt.Println("db reopen:", err, string(value))
	db2.close()
}
