package client

import (
	"benchmark/cache"
	bconfig "benchmark/config"
	metrics2 "benchmark/metrics"
	"benchmark/util"
	"context"
	"errors"
	"fmt"
	"sync"
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
	nodes []*cache.Node
	p     *bconfig.Config
	//nodeRing     *cache.NodeRing
	timers       []*time.Timer // Timers for scheduling node failures
	ctx          context.Context
	memNodes     map[string][]int
	mu           sync.Mutex // Mutex to protect memNodes
	cacheTimeout time.Duration
	failedNodes  map[int]bool
}

func NewCache(p *bconfig.Config, ctx context.Context) *CacheWrapper {
	c := CacheWrapper{}
	c.p = p
	c.ctx = ctx
	//c.nodeRing = cache.NewNodeRing(len(p.Cache.Nodes), p.Cache.VirtualNodes.Value, p.Cache.EnableReconfiguration.Value)
	c.memNodes = make(map[string][]int)
	c.cacheTimeout = time.Duration(1000) * time.Millisecond
	c.failedNodes = make(map[int]bool)
	c.nodes = make([]*cache.Node, 0, len(p.Cache.Nodes))

	for i := 0; i < len(p.Cache.Nodes); i++ {
		nodeConfig := p.Cache.Nodes[i]
		c.addNode(nodeConfig, ctx, len(p.Cache.Nodes))
		c.failedNodes[i] = false
	}
	for _, node := range c.nodes {
		node.SetOtherNodes(c.nodes)
	}
	updateInterval := time.Duration((time.Duration(p.Workload.TargetExecutionTime.Value)*time.Second).Milliseconds()/100) * time.Millisecond
	for _, node := range c.nodes {
		node.StartTopKeysUpdateTask(ctx, updateInterval)
	}
	go c.scheduleFailures()
	go c.scheduleCheckForRecoveries(ctx)
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
				//c.nodeRing.ReconfigureRingAfterFailure(nodeIndex)
				go nodeFailureMeasure(time.Now(), nodeIndex, true)

				// Schedule node recovery
				recoverTimer := time.AfterFunc(endDelay-startDelay, func() {
					go c.nodes[nodeIndex].Recover(c.ctx)
					//c.nodeRing.ReconfigureRingAfterRecovery(nodeIndex)
					nodeFailureMeasure(time.Now(), nodeIndex, false)
				})
				c.timers = append(c.timers, recoverTimer)
			})

			c.timers = append(c.timers, failTimer)
		}
	}
}

func (c *CacheWrapper) scheduleCheckForRecoveries(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for node := range c.failedNodes {
				c.mu.Lock()
				failed := c.failedNodes[node]
				c.mu.Unlock()
				if failed {
					go c.checkNodeRecovered(node, ctx)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// checkNodeRecovered checks if a given node has recovered.
func (c *CacheWrapper) checkNodeRecovered(node int, ctx context.Context) {
	if _, err, _ := c.nodes[node].GetWithTimeout(c.cacheTimeout, ctx, "key", []string{}); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		c.mu.Lock()
		c.failedNodes[node] = false
		c.mu.Unlock()
	}
}

func (c *CacheWrapper) addNode(p bconfig.NodeConfig, ctx context.Context, numBackUps int) {
	node := cache.NewNode(p, ctx, numBackUps)
	c.nodes = append(c.nodes, node)
}

func (c *CacheWrapper) Get(ctx context.Context, key string, fields []string) (map[string][]byte, error, int64) {

	nodes := c.GetNodes(key)

	var result map[string][]byte = nil
	var err error = nil
	var size int64 = 0
	var nodeId int = nodes[0]

	if !c.p.Cache.EnableReconfiguration.Value {
		nodes = []int{nodes[0]}
	}

	start := time.Now()

	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		c.mu.Lock()
		failed := c.failedNodes[node]
		c.mu.Unlock()
		if !failed {
			result, err, size = c.nodes[node].GetWithTimeout(c.cacheTimeout, ctx, key, fields)
			if err != nil && errors.Is(err, context.DeadlineExceeded) {
				c.mu.Lock()
				c.failedNodes[node] = true
				c.mu.Unlock()
			} else {
				nodeId = node
				break
			}
		}
	}

	go cacheMeasure(start, key, nodeId, metrics2.READ, err, size, c.nodes[nodeId].IsTopKey(key))
	return result, err, size
}

func (c *CacheWrapper) Set(ctx context.Context, key string, value map[string][]byte) (error, int64) {

	nodes := c.GetNodes(key)

	var err error = nil
	var size int64 = 0
	var nodeId int = nodes[0]

	start := time.Now()

	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		c.mu.Lock()
		failed := c.failedNodes[node]
		c.mu.Unlock()
		if !failed {
			err, size = c.nodes[node].SetWithTimeout(c.cacheTimeout, ctx, key, value)
			if err != nil && errors.Is(err, context.DeadlineExceeded) {
				c.mu.Lock()
				c.failedNodes[node] = true
				c.mu.Unlock()
			} else {
				nodeId = i
				break
			}
		}
	}

	go cacheMeasure(start, key, nodeId, metrics2.INSERT, err, size, c.nodes[nodeId].IsTopKey(key))
	return err, size
}

func (c *CacheWrapper) GetNodes(key string) []int {
	numbers := make([]int, 0, len(c.nodes))
	used := make(map[int]bool)
	hash := fmt.Sprintf("%d", util.StringHash64(key))
	hash2 := fmt.Sprintf("%d", util.StringHash64(hash))
	hash = hash + hash2

	for i := 0; i < len(hash); i++ {
		digit := int(hash[i]-'0') % len(c.nodes)
		if !used[digit] {
			numbers = append(numbers, digit)
			used[digit] = true
		}
		if len(numbers) == len(c.nodes) {
			break
		}
	}

	// Fill in any missing nodes
	for i := 0; len(numbers) < len(c.nodes); i++ {
		index := i % len(c.nodes)
		if !used[index] {
			numbers = append(numbers, index)
			used[index] = true
		}
	}

	return numbers
}
