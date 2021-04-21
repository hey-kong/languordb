package languordb

import (
	"sync"

	"github.com/hashicorp/golang-lru"
	"github.com/hey-kong/languordb/config"
	"github.com/hey-kong/languordb/errors"
)

type RowCache struct {
	mu     sync.Mutex
	dbName string
	cache  *lru.Cache
}

func NewRowCache(dbName string) *RowCache {
	var rowCache RowCache
	rowCache.dbName = dbName
	rowCache.cache, _ = lru.New(config.RowCacheSize)
	return &rowCache
}

func (rowCache *RowCache) Add(key, value []byte) {
	rowCache.cache.Add(string(key), string(value))
}

func (rowCache *RowCache) Get(key []byte) ([]byte, error) {
	rowCache.mu.Lock()
	defer rowCache.mu.Unlock()
	value, ok := rowCache.cache.Get(string(key))
	if ok {
		return []byte(value.(string)), nil
	}
	return nil, errors.ErrNotFound
}

func (rowCache *RowCache) Evict(key []byte) {
	rowCache.cache.Remove(string(key))
}
