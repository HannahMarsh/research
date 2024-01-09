package cache

import (
	bconfig "benchmark/config"
	"context"
)

type Cache struct {
	nodes []*CacheNode
	p     *bconfig.Config
}

func NewCache(p *bconfig.Config) *Cache {
	c := Cache{}
	c.p = p
	return &c
}

func (c *Cache) AddNode(url string, port string, maxSize int64, id int) {
	node := NewCacheWrapper(url, port, maxSize, id)
	c.nodes = append(c.nodes, node)
}

func (c *Cache) NumNodes() int {
	return len(c.nodes)
}

func (c *Cache) Get(ctx context.Context, key string, fields []string) (map[string][]byte, error) {
	return c.nodes[0].Get(ctx, key, fields)
}

func (c *Cache) Set(ctx context.Context, key string, value map[string][]byte) error {
	return c.nodes[0].Set(ctx, key, value)
}
