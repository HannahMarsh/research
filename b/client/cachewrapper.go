package client

import (
	"benchmark/cache"
	bconfig "benchmark/config"
	metrics2 "benchmark/metrics"
	"context"
	"time"
)

func cacheMeasure(start time.Time, nodeIndex int, operationType string, err error) {
	latency := time.Now().Sub(start)
	if err != nil {
		metrics2.AddMeasurement(metrics2.CACHE_OPERATION, start,
			map[string]string{
				"successful": "false",
				"operation":  operationType,
				"error":      err.Error(),
			},
			map[string]float64{
				"latency":   latency.Seconds(),
				"nodeIndex": float64(nodeIndex),
			})
		return
	} else {
		metrics2.AddMeasurement(metrics2.CACHE_OPERATION, start,
			map[string]string{
				"successful": "true",
				"operation":  operationType,
			},
			map[string]float64{
				"latency":   latency.Seconds(),
				"nodeIndex": float64(nodeIndex),
			})
	}
}

type CacheWrapper struct {
	nodes    []*cache.Node
	p        *bconfig.Config
	nodeRing *cache.NodeRing
}

func NewCache(p *bconfig.Config) *CacheWrapper {
	c := CacheWrapper{}
	c.p = p
	c.nodeRing = cache.NewNodeRing(len(p.Cache.Nodes), p.Cache.VirtualNodes.Value)

	for i := 0; i < len(p.Cache.Nodes); i++ {
		nodeConfig := p.Cache.Nodes[i]
		c.addNode(nodeConfig.Address.Value, nodeConfig.MaxSize.Value, nodeConfig.NodeId.Value)
	}

	return &c
}

func (c *CacheWrapper) addNode(address string, maxSize int, id int) {
	node := cache.NewNode(address, int64(maxSize), id)
	c.nodes = append(c.nodes, node)
}

func (c *CacheWrapper) NumNodes() int {
	return len(c.nodes)
}

func (c *CacheWrapper) Get(ctx context.Context, key string, fields []string) (_ map[string][]byte, err error) {

	start := time.Now()
	nodeIndex := c.nodeRing.GetNode(key)

	defer func() {
		cacheMeasure(start, nodeIndex, "GET", err)
	}()

	return c.nodes[nodeIndex].Get(ctx, key, fields)
}

func (c *CacheWrapper) Set(ctx context.Context, key string, value map[string][]byte) (err error) {

	start := time.Now()
	nodeIndex := c.nodeRing.GetNode(key)

	defer func() {
		cacheMeasure(start, nodeIndex, "GET", err)
	}()

	return c.nodes[nodeIndex].Set(ctx, key, value)
}
