# languordb
LanguorDB is a simple key-value database written in pure Go, based on [LevelDB](https://github.com/google/leveldb). It is not for production use. Instead, it is designed as my bachelor's degree final project titled 'Optimization of Key-Value Database by Coarse-grain Compaction'.

## Compared Go Key-Value Store

* [BoltDB](https://github.com/boltdb/bolt) (v1.3.1, default options)
* [LevelDB](https://github.com/syndtr/goleveldb) (v1.0.0, default options with sync)
* [LanguorDB](https://github.com/hey-kong/languordb) (master branch, default options)

## Benchmark System

* Go Version : go1.16.3 linux/amd64
* OS: CentOS  7.6 64-bit
* Architecture: x86_64
* CPU and Memory: 8Cores 16 GiB

## Benchmark results

### Put(write) Performance

```
goos: linux
goarch: amd64
pkg: github.com/hey-kong/languordb-bench
cpu: Intel(R) Xeon(R) Platinum 8269CY CPU @ 2.50GHz
BenchmarkBoltDBPutValue64B-8       	   50000	   1156896 ns/op	   22851 B/op	      62 allocs/op
BenchmarkBoltDBPutValue128B-8      	   50000	   1638081 ns/op	   15337 B/op	      62 allocs/op
BenchmarkBoltDBPutValue256B-8      	   50000	    918098 ns/op	   18456 B/op	      62 allocs/op
BenchmarkBoltDBPutValue512B-8      	   50000	   1823506 ns/op	   22360 B/op	      62 allocs/op
BenchmarkLevelDBPutValue64B-8      	   50000	    990415 ns/op	     385 B/op	       6 allocs/op
BenchmarkLevelDBPutValue128B-8     	   50000	    990833 ns/op	     632 B/op	       7 allocs/op
BenchmarkLevelDBPutValue256B-8     	   50000	    991970 ns/op	     824 B/op	       7 allocs/op
BenchmarkLevelDBPutValue512B-8     	   50000	    992508 ns/op	     866 B/op	       7 allocs/op
BenchmarkLanguorDBPutValue64B-8    	   50000	    555006 ns/op	     849 B/op	      24 allocs/op
BenchmarkLanguorDBPutValue128B-8   	   50000	    554985 ns/op	     954 B/op	      18 allocs/op
BenchmarkLanguorDBPutValue256B-8   	   50000	    656524 ns/op	    4963 B/op	      79 allocs/op
BenchmarkLanguorDBPutValue512B-8   	   50000	    728433 ns/op	    6181 B/op	      54 allocs/op
PASS
ok  	github.com/hey-kong/languordb-bench	599.942s
```

### Get(read) Performance

```
goos: linux
goarch: amd64
pkg: github.com/hey-kong/languordb-bench
cpu: Intel(R) Xeon(R) Platinum 8269CY CPU @ 2.50GHz
BenchmarkBoltDBGet-8      	 1000000	      1275 ns/op	     574 B/op	       9 allocs/op
BenchmarkLevelDBGet-8     	 1000000	      2885 ns/op	     754 B/op	      13 allocs/op
BenchmarkLanguorDBGet-8   	 1000000	       993.3 ns/op	     316 B/op	      10 allocs/op
PASS
ok  	github.com/hey-kong/languordb-bench	334.036s
```
