package node

import (
	"node/util"
	"sync"
)

// FNV-1a constants
const (
	offset64 uint64 = 14695981039346656037
	prime64  uint64 = 1099511628211
)

type LFUCache struct {
	capacity int
	items    []map[string]util.Data
	locks    []sync.RWMutex
	heap     *util.ConcurrentQueue
	curSize  int
	curLock  sync.RWMutex
}

func NewLFUCache(capacity int) *LFUCache {
	return &LFUCache{
		capacity: capacity,
		items:    make([]map[string]util.Data, capacity),
		heap:     util.NewConcurrentQueue(capacity),
		locks:    make([]sync.RWMutex, capacity),
		curSize:  0,
	}
}

func (c *LFUCache) Get(key string) (value interface{}, ok bool) {
	index := c.hash(key)

	c.locks[index].RLock()
	defer c.locks[index].RUnlock()

	if section := c.items[index]; section != nil {
		if item, exists := section[key]; exists {
			item.Freq++
			return item.Value, true
		}
	}
	return nil, false
}

func (c *LFUCache) Set(key string, value interface{}) {
	index := c.hash(key)

	c.locks[index].Lock()
	defer c.locks[index].Unlock()

	if c.items[index] == nil {
		c.items[index] = make(map[string]util.Data)
	}

	item, exists := c.items[index][key]

	if !exists { // if the key doesn't already exist, we need to add it

		evict := false

		c.curLock.RLock()
		if c.curSize >= c.capacity { // if full
			evict = true
		}
		c.curLock.RUnlock()

		if !evict {
			c.curLock.Lock()
			if c.curSize < c.capacity { // if not full
				c.curSize++
			} else {
				evict = true
			}
			c.curLock.Unlock()
		}

		if evict { // Evict least frequently accessed item
			// evicted := &(heap.Dequeue())
			// delete(c.items[evicted.hash], evicted.Key)
		}

		c.items[index][key] = util.Data{Key: key, Value: value, Hash: index, Index: index, Freq: 1}
	} else {
		item.Value = value
		item.Hash = index
		item.Index = index
		item.Freq++
		// heap.Fix(&c.heap, item.index)
	}
	// heap.Push(&c.heap, c.items[index][key])
}

func (c *LFUCache) hash(key string) int {

	hash := offset64
	for _, c := range key {
		hash ^= uint64(c)
		hash *= prime64
	}
	return int(hash) % c.capacity
}
