package node

import (
	"node/util"
)

type LFUCache struct {
	capacity int
	cq       *util.ConcurrentQueue
}

func NewLFUCache(capacity int) *LFUCache {
	return &LFUCache{
		capacity: capacity,
		cq:       util.NewConcurrentQueue(capacity),
	}
}

func (c *LFUCache) Get(key string) (value interface{}, ok bool) {
	if data, present := c.cq.Get(key); present {
		return data.Value, true
	}
	return nil, false
}

func (c *LFUCache) Set(key string, value interface{}, primaryNode int, backUpNode int) {
	data := util.Data{Key: key, Value: value, PrimaryNode: primaryNode, BackUpNode: backUpNode}
	c.cq.Set(key, data)
}

func (c *LFUCache) GetTop(n int) map[int]map[string]interface{} {
	return c.cq.GetTop(n)
}

//// FNV-1a constants
//const (
//	offset64 uint64 = 14695981039346656037
//	prime64  uint64 = 1099511628211
//)
//
//func (c *LFUCache) hash(key string) int {
//
//	hash := offset64
//	for _, c := range key {
//		hash ^= uint64(c)
//		hash *= prime64
//	}
//	return int(hash) % c.capacity
//}
