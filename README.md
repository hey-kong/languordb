# languordb
LanguorDB is a simple key-value database written in Go, based on [LevelDB](https://github.com/google/leveldb). It is not for production use. Instead, it is designed as my bachelor's degree final project titled 'Optimization of Key-Value Database by Coarse-grain Compaction'.

## Benchmark System

* Go Version : go1.16.3 linux/amd64
* OS: CentOS  7.6 64-bit
* Architecture: x86_64
* CPU and Memory: 8Cores 16 GiB

## To run benchmark

View the benchmark repo [here](https://github.com/hey-kong/languordb-bench).

>go test -bench=Put -benchtime=50000x
>
>go test -bench=Get -benchtime=1000000x

## Benchmark results

### Put(write) Performance

```
goos: linux
goarch: amd64
pkg: github.com/hey-kong/languordb-bench
cpu: Intel(R) Xeon(R) Platinum 8269CY CPU @ 2.50GHz
BenchmarkBoltDBPutValue64B-8       	   50000	   1156086 ns/op	   22871 B/op	      62 allocs/op
BenchmarkBoltDBPutValue128B-8      	   50000	   1638158 ns/op	   15355 B/op	      62 allocs/op
BenchmarkBoltDBPutValue256B-8      	   50000	    918135 ns/op	   18476 B/op	      62 allocs/op
BenchmarkBoltDBPutValue512B-8      	   50000	   1823573 ns/op	   22382 B/op	      62 allocs/op
BenchmarkLevelDBPutValue64B-8      	   50000	       795.2 ns/op	     219 B/op	       7 allocs/op
BenchmarkLevelDBPutValue128B-8     	   50000	     19840 ns/op	     438 B/op	      15 allocs/op
BenchmarkLevelDBPutValue256B-8     	   50000	     78592 ns/op	     881 B/op	      23 allocs/op
BenchmarkLevelDBPutValue512B-8     	   50000	    560008 ns/op	   12865 B/op	     245 allocs/op
BenchmarkLanguorDBPutValue64B-8    	   50000	       809.6 ns/op	     225 B/op	       7 allocs/op
BenchmarkLanguorDBPutValue128B-8   	   50000	     14889 ns/op	     788 B/op	      22 allocs/op
BenchmarkLanguorDBPutValue256B-8   	   50000	    103471 ns/op	    3621 B/op	      81 allocs/op
BenchmarkLanguorDBPutValue512B-8   	   50000	    213338 ns/op	    3922 B/op	      52 allocs/op
PASS
ok  	github.com/hey-kong/languordb-bench	326.896s
```

### Get(read) Performance

```
goos: linux
goarch: amd64
pkg: github.com/hey-kong/languordb-bench
cpu: Intel(R) Xeon(R) Platinum 8269CY CPU @ 2.50GHz
BenchmarkBoltDBGet-8      	 1000000	      1280 ns/op	     574 B/op	       9 allocs/op
BenchmarkLevelDBGet-8     	 1000000	     32191 ns/op	   26584 B/op	     557 allocs/op
BenchmarkLanguorDBGet-8   	 1000000	       959.2 ns/op	     292 B/op	      10 allocs/op
PASS
ok  	github.com/hey-kong/languordb-bench	240.878s
```