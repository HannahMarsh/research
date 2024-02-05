package client

import (
	"benchmark/cache"
	bconfig "benchmark/config"
	metrics2 "benchmark/metrics"
	"context"
	"time"
)

func cacheMeasure(start time.Time, key string, nodeIndex int, operationType string, err error, cacheSize int64, isHottest bool) {
	latency := time.Now().Sub(start)
	if err != nil {
		metrics2.AddMeasurement(metrics2.CACHE_OPERATION, start,
			map[string]interface{}{
				metrics2.SUCCESSFUL: false,
				metrics2.OPERATION:  operationType,
				metrics2.ERROR:      err.Error(),
				metrics2.LATENCY:    latency.Seconds(),
				metrics2.NODE_INDEX: nodeIndex,
				metrics2.SIZE:       cacheSize,
				metrics2.KEY:        key,
				metrics2.HOTTEST:    isHottest,
			})
		return
	} else {
		metrics2.AddMeasurement(metrics2.CACHE_OPERATION, start,
			map[string]interface{}{
				metrics2.SUCCESSFUL: true,
				metrics2.OPERATION:  operationType,
				metrics2.LATENCY:    latency.Seconds(),
				metrics2.NODE_INDEX: nodeIndex,
				metrics2.SIZE:       cacheSize,
				metrics2.KEY:        key,
				metrics2.HOTTEST:    isHottest,
			})
	}
}

func nodeFailureMeasure(t time.Time, nodeIndex int, isStart bool) {
	if isStart {
		metrics2.AddMeasurement(metrics2.NODE_FAILURE, t,
			map[string]interface{}{
				metrics2.INTERVAL:   metrics2.START,
				metrics2.NODE_INDEX: nodeIndex,
			})
	} else {
		metrics2.AddMeasurement(metrics2.NODE_FAILURE, t,
			map[string]interface{}{
				metrics2.INTERVAL:   metrics2.END,
				metrics2.NODE_INDEX: nodeIndex,
			})
	}
}

type CacheWrapper struct {
	nodes    []*cache.Node
	p        *bconfig.Config
	nodeRing *cache.NodeRing
	timers   []*time.Timer // Timers for scheduling node failures
	ctx      context.Context
}

func NewCache(p *bconfig.Config, ctx context.Context) *CacheWrapper {
	c := CacheWrapper{}
	c.p = p
	c.ctx = ctx
	c.nodeRing = cache.NewNodeRing(len(p.Cache.Nodes), p.Cache.VirtualNodes.Value, p.Cache.EnableReconfiguration.Value)
	//backUpSize := bconfig.IntProperty{Value: 100}
	////backUpSize := bconfig.IntProperty{Value: p.Cache.NumHottestKeysBackup.Value / 1000}
	//c.hottestKeys = cache.NewNode(bconfig.NodeConfig{
	//	Address:            p.Cache.BackUpAddress,
	//	MaxMemoryMbs:       backUpSize,
	//	MaxMemoryPolicy:    p.Cache.Nodes[0].MaxMemoryPolicy,
	//	UseDefaultDatabase: p.Cache.Nodes[0].UseDefaultDatabase,
	//}, ctx)

	for i := 0; i < len(p.Cache.Nodes); i++ {
		nodeConfig := p.Cache.Nodes[i]
		c.addNode(nodeConfig, ctx, len(p.Cache.Nodes))
	}
	for _, node := range c.nodes {
		node.SetOtherNodes(c.nodes)
	}
	updateInterval := time.Duration((time.Duration(p.Workload.TargetExecutionTime.Value)*time.Second).Milliseconds()/100) * time.Millisecond
	for _, node := range c.nodes {
		node.StartTopKeysUpdateTask(ctx, updateInterval)
	}
	c.scheduleFailures()
	return &c
}

func (c *CacheWrapper) scheduleFailures() {
	for i := 0; i < len(c.p.Cache.Nodes); i++ {
		nodeIndex := i

		warmUpTime := time.Duration(c.p.Measurements.WarmUpTime.Value) * time.Second
		targetRunningTime := float64(c.p.Workload.TargetExecutionTime.Value)
		for j := 0; j < len(c.p.Cache.Nodes[nodeIndex].FailureIntervals); j++ {
			failureIndex := j
			interval := c.p.Cache.Nodes[nodeIndex].FailureIntervals[failureIndex]
			startDelay := (time.Duration(interval.Start*targetRunningTime) * time.Second) + warmUpTime
			endDelay := (time.Duration(interval.End*targetRunningTime) * time.Second) + warmUpTime

			// Schedule node failure
			failTimer := time.AfterFunc(startDelay, func() {
				go c.nodes[nodeIndex].Fail()
				c.nodeRing.ReconfigureRingAfterFailure(nodeIndex)
				go nodeFailureMeasure(time.Now(), nodeIndex, true)

				// Schedule node recovery
				recoverTimer := time.AfterFunc(endDelay-startDelay, func() {
					go c.nodes[nodeIndex].Recover(c.ctx)
					c.nodeRing.ReconfigureRingAfterRecovery(nodeIndex)
					nodeFailureMeasure(time.Now(), nodeIndex, false)
				})
				c.timers = append(c.timers, recoverTimer)
			})

			c.timers = append(c.timers, failTimer)
		}
	}
}

func (c *CacheWrapper) addNode(p bconfig.NodeConfig, ctx context.Context, numBackUps int) {
	node := cache.NewNode(p, ctx, numBackUps)
	c.nodes = append(c.nodes, node)
}

func (c *CacheWrapper) NumNodes() int {
	return len(c.nodes)
}

func (c *CacheWrapper) Get(ctx context.Context, key string, fields []string) (_ map[string][]byte, err error, size int64) {

	start := time.Now()

	failedNodeIndex, backup, isBackup := c.nodeRing.GetNode(key)

	defer func() {
		cacheMeasure(start, key, backup, metrics2.READ, err, size, c.nodes[backup].IsTopKey(key))
	}()

	if isBackup {
		return c.nodes[backup].GetBackup(ctx, failedNodeIndex, key, fields)
	} else {
		return c.nodes[backup].Get(ctx, key, fields)
	}
}

func (c *CacheWrapper) Set(ctx context.Context, key string, value map[string][]byte) (err error, size int64) {

	start := time.Now()

	_, backup, _ := c.nodeRing.GetNode(key)

	defer func() {
		cacheMeasure(start, key, backup, metrics2.INSERT, err, size, c.nodes[backup].IsTopKey(key))
	}()

	return c.nodes[backup].Set(ctx, key, value)

	//if isBackup {
	//	return c.nodes[backup].SetBackup(ctx, failedNodeIndex, key, value)
	//} else {
	//	return c.nodes[backup].Set(ctx, key, value)
	//}
}
