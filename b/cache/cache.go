package cache

import (
	bconfig "benchmark/config"
	"context"
)

type Cache struct {
	nodes    []*CacheNode
	p        *bconfig.Config
	nodeRing *NodeRing
}

func NewCache(p *bconfig.Config) *Cache {
	c := Cache{}
	c.p = p
	c.nodeRing = NewNodeRing(len(p.Cache.Nodes), p.Cache.VirtualNodes.Value)

	for i := 0; i < len(p.Cache.Nodes); i++ {
		nodeConfig := p.Cache.Nodes[i]
		c.AddNode(nodeConfig.Address.Value, nodeConfig.MaxSize.Value, nodeConfig.NodeId.Value)
	}

	return &c
}

func (c *Cache) AddNode(address string, maxSize int, id int) {
	node := NewCacheWrapper(address, int64(maxSize), id)
	c.nodes = append(c.nodes, node)
}

func (c *Cache) NumNodes() int {
	return len(c.nodes)
}

func (c *Cache) Get(ctx context.Context, key string, fields []string) (map[string][]byte, error) {
	nodeIndex := c.nodeRing.GetNode(key)
	return c.nodes[nodeIndex].Get(ctx, key, fields)
}

func (c *Cache) Set(ctx context.Context, key string, value map[string][]byte) error {
	nodeIndex := c.nodeRing.GetNode(key)
	return c.nodes[nodeIndex].Set(ctx, key, value)
}
