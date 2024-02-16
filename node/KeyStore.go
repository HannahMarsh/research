package node

import (
	"container/heap"
	"sync"
)

// FNV-1a constants
const (
	offset64 uint64 = 14695981039346656037
	prime64  uint64 = 1099511628211
)

type Data struct {
	key   string
	value interface{}
	freq  int
	index int // Index in the heap
	hash  int
}

type LFUCache struct {
	capacity int
	items    []map[string]Data
	locks    []sync.RWMutex
	heap     itemHeap
	curSize  int
	curLock  sync.RWMutex
}

func NewLFUCache(capacity int) *LFUCache {
	return &LFUCache{
		capacity: capacity,
		items:    make([]map[string]Data, capacity),
		heap:     make(itemHeap, 0, capacity),
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
			item.freq++
			heap.Fix(&c.heap, item.index)
			return item.value, true
		}
	}
	return nil, false
}

func (c *LFUCache) Set(key string, value interface{}) {
	index := c.hash(key)

	c.locks[index].Lock()
	defer c.locks[index].Unlock()

	if c.items[index] == nil {
		c.items[index] = make(map[string]Data)
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
			evicted := heap.Pop(&c.heap).(*Data)
			delete(c.items[evicted.hash], evicted.key)
		}

		c.items[index][key] = Data{key: key, value: value, hash: index, index: index, freq: 1}
	} else {
		item.value = value
		item.hash = index
		item.index = index
		item.freq++
		heap.Fix(&c.heap, item.index)
	}
	heap.Push(&c.heap, c.items[index][key])
}

func (c *LFUCache) hash(key string) int {

	hash := offset64
	for _, c := range key {
		hash ^= uint64(c)
		hash *= prime64
	}
	return int(hash) % c.capacity
}
