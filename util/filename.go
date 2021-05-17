package util

import (
	"fmt"
	"os"
)

func IsExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func TableFileName(dbname string, number uint64) string {
	return makeFileName(dbname, number, "ldb")
}

func WalFileName(dbname string) string {
	return fmt.Sprintf("%s/mock.%s", dbname, "log")
}

func DescriptorFileName(dbname string, number uint64) string {
	return fmt.Sprintf("%s/MANIFEST-%06d", dbname, number)
}

func CurrentFileName(dbname string) string {
	return dbname + "/CURRENT"
}

func LogFileName(dbname string) string {
	return dbname + "/LOG"
}

func TempFileName(dbname string, number uint64) string {
	return makeFileName(dbname, number, "dbtmp")
}

func makeFileName(dbname string, number uint64, suffix string) string {
	return fmt.Sprintf("%s/%06d.%s", dbname, number, suffix)
}
