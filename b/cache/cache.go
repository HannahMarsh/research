package cache

import (
	"context"
	"errors"
	"github.com/eko/gocache/lib/v4/cache"
	gocachestore "github.com/eko/gocache/store/go_cache/v4"
	gocache "github.com/patrickmn/go-cache"
	"log"
	"sync"
	"time"
)

type CacheWrapper struct {
	isFailed       bool
	failMutex      sync.Mutex
	cacheManager   *cache.Cache[string]
	geocacheClient *gocache.Cache
	maxSize        int64
}

func NewCacheWrapper(url string, port string, defaultExpiration, cleanupInterval time.Duration, maxSize int64) *CacheWrapper {

	c := new(CacheWrapper)
	// Initialize Gocache in-memory store
	c.geocacheClient = gocache.New(defaultExpiration, cleanupInterval) // default
	geocacheStore := gocachestore.NewGoCache(c.geocacheClient)

	// create new cache manager
	c.cacheManager = cache.New[string](geocacheStore)

	c.isFailed = false
	c.maxSize = maxSize

	return c
}

func (c *CacheWrapper) Size() int {
	return c.geocacheClient.ItemCount()
}

// Recover simulates cache recovery from failure
func (c *CacheWrapper) Recover() {
	c.failMutex.Lock()
	defer c.failMutex.Unlock()
	c.isFailed = false

	// clear the cache to simulate an empty state
	err := c.cacheManager.Clear(context.Background())
	if err != nil {
		log.Printf("Failed to clear cache: %v", err)
		return
	}
}

// Fail simulates a cache node failure
func (c *CacheWrapper) Fail() {
	c.failMutex.Lock()
	defer c.failMutex.Unlock()
	c.isFailed = true
}

// Get handles the cache get requests
func (c *CacheWrapper) Get(key string) (string, int, error) {
	if c.isFailed {
		return "", 0, errors.New("simulated failure - cache node is not available")
	}
	size := c.Size()
	if size >= int(c.maxSize) {
		return "", size, errors.New("cache is full")
	}
	val, err := c.cacheManager.Get(context.Background(), key)
	return val, size, err
}

// Set handles the cache set requests
func (c *CacheWrapper) Set(key string, value string) (int, error) {
	if c.isFailed {
		return 0, errors.New("simulated failure - cache node is not available")
	}
	size := c.Size()
	if size >= int(c.maxSize) {
		return size, errors.New("cache is full")
	}
	return size, c.cacheManager.Set(context.Background(), key, value)
}
