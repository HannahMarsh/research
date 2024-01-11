package client

import (
	"benchmark/cache"
	bconfig "benchmark/config"
	metrics2 "benchmark/metrics"
	"context"
	"fmt"
	"time"
)

func cacheMeasure(start time.Time, nodeIndex int, operationType string, err error) {
	latency := time.Now().Sub(start)
	if err != nil {
		metrics2.AddMeasurement(metrics2.CACHE_OPERATION, start,
			map[string]interface{}{
				metrics2.SUCCESSFUL: false,
				metrics2.OPERATION:  operationType,
				metrics2.ERROR:      err.Error(),
				metrics2.LATENCY:    latency.Seconds(),
				metrics2.NODE_INDEX: nodeIndex,
			})
		return
	} else {
		metrics2.AddMeasurement(metrics2.CACHE_OPERATION, start,
			map[string]interface{}{
				metrics2.SUCCESSFUL: true,
				metrics2.OPERATION:  operationType,
				metrics2.LATENCY:    latency.Seconds(),
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
	c.nodeRing = cache.NewNodeRing(len(p.Cache.Nodes), p.Cache.VirtualNodes.Value)

	for i := 0; i < len(p.Cache.Nodes); i++ {
		nodeConfig := p.Cache.Nodes[i]
		c.addNode(nodeConfig.Address.Value, nodeConfig.MaxSize.Value, nodeConfig.NodeId.Value)
		c.scheduleFailures(i, nodeConfig.FailureIntervals)
	}

	t := EstimateRunningTime(p)
	fmt.Printf("Estimated running time: %v\n", t)

	return &c
}

//func EstimateRunningTime(config *bconfig.Config) time.Duration {
//	var totalOpCount int64
//	if config.Workload.DoTransactions.Value {
//		totalOpCount = int64(config.Performance.OperationCount.Value)
//	} else {
//		if config.Performance.InsertCount.Value > 0 {
//			totalOpCount = int64(config.Performance.InsertCount.Value)
//		} else {
//			totalOpCount = int64(config.Performance.RecordCount.Value)
//		}
//	}
//
//	batchSize := 1
//	if config.Performance.BatchSize.Value > 1 {
//		batchSize = config.Performance.BatchSize.Value
//	}
//
//	totalDBInteractions := totalOpCount / int64(batchSize)
//
//	targetOpsPerSec := float64(config.Performance.TargetOperationsPerSec.Value)
//	if targetOpsPerSec <= 0 {
//		targetOpsPerSec = 1 // Set a default value if not specified
//	}
//
//	timePerOp := time.Second / time.Duration(targetOpsPerSec)
//	estimatedDuration := timePerOp * time.Duration(totalDBInteractions)
//
//	// Adjust for any additional delays (e.g., throttling, retries)
//	// This is a rough estimate and will depend on your specific implementation details
//	adjustmentFactor := 1.0 // Adjust this based on expected delays
//	estimatedDuration = time.Duration(float64(estimatedDuration) * adjustmentFactor)
//
//	return estimatedDuration
//}

func (c *CacheWrapper) scheduleFailures(nodeIndex int, intervals []bconfig.FailureInterval) {
	for _, interval := range intervals {
		startDelay := time.Duration(interval.Start * float64(time.Second))
		endDelay := time.Duration(interval.End * float64(time.Second))

		// Schedule node failure
		failTimer := time.AfterFunc(startDelay, func() {
			c.nodes[nodeIndex].Fail()
			// Schedule node recovery
			recoverTimer := time.AfterFunc(endDelay-startDelay, func() {
				c.nodes[nodeIndex].Recover(c.ctx)
			})
			c.timers = append(c.timers, recoverTimer)
		})

		c.timers = append(c.timers, failTimer)
	}
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
